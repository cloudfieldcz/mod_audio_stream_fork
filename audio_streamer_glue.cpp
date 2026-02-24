#include <string>
#include <cstring>
#include "mod_audio_stream.h"
#include "WebSocketClient.h"
#include <switch_json.h>
#include <fstream>
#include <switch_buffer.h>
#include <unordered_set>
#include <atomic>
#include <vector>
#include <memory>
#include <mutex>
#include "base64.h"

#define FRAME_SIZE_8000  320 /* 1000x0.02 (20ms)= 160 x(16bit= 2 bytes) 320 frame size*/

class AudioStreamer {
public:
    // Factory
    static std::shared_ptr<AudioStreamer> create(
        const char* uuid, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers, const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation) {

        std::shared_ptr<AudioStreamer> sp(new AudioStreamer(
            uuid, wsUri, callback, deflate, heart_beat,
            suppressLog, extra_headers, tls_cafile, tls_keyfile,
            tls_certfile, tls_disable_hostname_validation
        ));

        sp->bindCallbacks(std::weak_ptr<AudioStreamer>(sp));

        sp->client.connect();

        return sp;
    }

    ~AudioStreamer()= default;

    void disconnect() {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "disconnecting...\n");
        client.disconnect();
    }

    bool isConnected() {
        return client.isConnected();
    }

    void writeBinary(uint8_t* buffer, size_t len) {
        if(!this->isConnected()) return;
        client.sendBinary(buffer, len);
    }

    void writeText(const char* text) {
        if(!this->isConnected()) return;
        client.sendMessage(text, strlen(text));
    }

    void deleteFiles() {
        std::vector<std::string> files;

        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            if (m_Files.empty())
                return;

            files.assign(m_Files.begin(), m_Files.end());
            m_Files.clear();
            m_playFile = 0;
        }

        for (const auto& fn : files) {
            ::remove(fn.c_str());
        }
    }

    void markCleanedUp() {
        m_cleanedUp.store(true, std::memory_order_release);
        client.setMessageCallback({});
        client.setBinaryCallback({});
        client.setOpenCallback({});
        client.setErrorCallback({});
        client.setCloseCallback({});
    }

    bool isCleanedUp() const {
        return m_cleanedUp.load(std::memory_order_acquire);
    }

private:
    // Ctor
    AudioStreamer(
        const char* uuid, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers, const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation
    ) : m_sessionId(uuid), m_notify(callback), m_suppress_log(suppressLog),
        m_extra_headers(extra_headers), m_playFile(0) {

        WebSocketHeaders hdrs;
        WebSocketTLSOptions tls;

        if (m_extra_headers) {
            cJSON *headers_json = cJSON_Parse(m_extra_headers);
            if (headers_json) {
                cJSON *iterator = headers_json->child;
                while (iterator) {
                    if (iterator->type == cJSON_String && iterator->valuestring != nullptr) {
                        hdrs.set(iterator->string, iterator->valuestring);
                    }
                    iterator = iterator->next;
                }
                cJSON_Delete(headers_json);
            }
        }

        client.setUrl(wsUri);

        // Setup TLS options
        // NONE - disables validation
        // SYSTEM - uses the system CAs bundle
        if (tls_cafile) {
            tls.caFile = tls_cafile;
        }

        if (tls_keyfile) {
            tls.keyFile = tls_keyfile;
        }

        if (tls_certfile) {
            tls.certFile = tls_certfile;
        }

        tls.disableHostnameValidation = tls_disable_hostname_validation;
        client.setTLSOptions(tls);

        // Optional heart beat, sent every xx seconds when there is not any traffic
        // to make sure that load balancers do not kill an idle connection.
        if(heart_beat)
            client.setPingInterval(heart_beat);

        // Per message deflate connection is enabled by default. You can tweak its parameters or disable it
        if(deflate)
            client.enableCompression(false);

        // Set extra headers if any
        if(!hdrs.empty())
            client.setHeaders(hdrs);
    }

    struct ProcessResult {
        switch_bool_t ok = SWITCH_FALSE;
        std::string rewrittenJsonData;
        std::vector<std::string> errors;
    };

    static inline void push_err(ProcessResult& out, const std::string& sid, const std::string& s) {
        out.errors.push_back("(" + sid + ") " + s);
    }

    void bindCallbacks(std::weak_ptr<AudioStreamer> wp) {
        client.setMessageCallback([wp](const std::string& message) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;
            self->eventCallback(MESSAGE, message.c_str());
        });

        /* WRITE_REPLACE: handle incoming binary WebSocket frames as raw audio */
        client.setBinaryCallback([wp](const void* data, size_t len) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;
            self->onBinaryReceived(data, len);
        });

        client.setOpenCallback([wp]() {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "connected");
            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_SUCCESS, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client.setErrorCallback([wp](int code, const std::string& msg) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "error");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "error", msg.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_ERROR, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client.setCloseCallback([wp](int code, const std::string& reason) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "disconnected");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "reason", reason.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECTION_DROPPED, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });
    }

    switch_media_bug_t *get_media_bug(switch_core_session_t *session) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        if(!channel) {
            return nullptr;
        }
        auto *bug = (switch_media_bug_t *) switch_channel_get_private(channel, MY_BUG_NAME);
        return bug;
    }

    inline void media_bug_close(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if(bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            tech_pvt->close_requested = 1;
            switch_core_media_bug_close(&bug, SWITCH_FALSE);
        }
    }

    inline void send_initial_metadata(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if(bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            if(tech_pvt && strlen(tech_pvt->initialMetadata) > 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                                          "sending initial metadata %s\n", tech_pvt->initialMetadata);
                writeText(tech_pvt->initialMetadata);
            }
        }
    }

    /**
     * Handle incoming binary WebSocket frame: queue raw PCM16 audio into
     * the write buffer for WRITE_REPLACE injection.
     *
     * The binary data is expected to be raw PCM16 at the sampling rate
     * specified in the start command (e.g. 16kHz).
     */
    void onBinaryReceived(const void* data, size_t len) {
        if (len == 0) return;

        switch_core_session_t* psession = switch_core_session_locate(m_sessionId.c_str());
        if (!psession) return;

        auto *bug = get_media_bug(psession);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
                "onBinaryReceived: no media bug for session %s\n", m_sessionId.c_str());
            switch_core_session_rwunlock(psession);
            return;
        }

        auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt || !tech_pvt->wbuffer) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
                "onBinaryReceived: no tech_pvt or wbuffer for session %s\n", m_sessionId.c_str());
            switch_core_session_rwunlock(psession);
            return;
        }

        switch_mutex_lock(tech_pvt->write_mutex);
        tech_pvt->write_active = 1;
        switch_buffer_write(tech_pvt->wbuffer, data, len);
        switch_size_t inuse = switch_buffer_inuse(tech_pvt->wbuffer);
        switch_mutex_unlock(tech_pvt->write_mutex);

        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
            "onBinaryReceived: %zu bytes, wbuffer inuse=%zu\n", len, (size_t)inuse);

        switch_core_session_rwunlock(psession);
    }

    void eventCallback(notifyEvent_t event, const char* message) {
        std::string msg = message ? message : "";

        // processing without holding a session
        ProcessResult pr;
        if (event == MESSAGE) {
            pr = processMessage(msg);
            if (pr.ok == SWITCH_TRUE) {
                msg = pr.rewrittenJsonData; // overwrite only on success
            }
        }

        switch_core_session_t* psession = switch_core_session_locate(m_sessionId.c_str());
        if (!psession) {
            return;
        }

        for (const auto& e : pr.errors) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_ERROR, "%s\n", e.c_str());
        }

        switch (event) {
            case CONNECT_SUCCESS:
                send_initial_metadata(psession);
                m_notify(psession, EVENT_CONNECT, msg.c_str());
                break;

            case CONNECTION_DROPPED:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO, "connection closed\n");
                m_notify(psession, EVENT_DISCONNECT, msg.c_str());
                break;

            case CONNECT_ERROR:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO, "connection error\n");
                m_notify(psession, EVENT_ERROR, msg.c_str());
                media_bug_close(psession);
                break;

            case MESSAGE:
                if (pr.ok == SWITCH_TRUE) {
                    m_notify(psession, EVENT_PLAY, msg.c_str());
                } else {
                    // fall back to EVENT_JSON
                    m_notify(psession, EVENT_JSON, msg.c_str());
                }

                if (!m_suppress_log) {
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_DEBUG,
                                    "response: %s\n", msg.c_str());
                }
                break;
        }

        switch_core_session_rwunlock(psession);
    }


    /**
     * Flush the write buffer — called on barge-in to stop queued audio
     * from continuing to play after the user interrupts.
     */
    void clearWriteBuffer() {
        switch_core_session_t* psession = switch_core_session_locate(m_sessionId.c_str());
        if (!psession) return;

        auto *bug = get_media_bug(psession);
        if (bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            if (tech_pvt && tech_pvt->wbuffer) {
                switch_mutex_lock(tech_pvt->write_mutex);
                switch_buffer_zero(tech_pvt->wbuffer);
                switch_mutex_unlock(tech_pvt->write_mutex);
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_DEBUG,
                                  "(%s) write buffer cleared (barge-in)\n", m_sessionId.c_str());
            }
        }

        switch_core_session_rwunlock(psession);
    }

    ProcessResult processMessage(const std::string& message) {
        ProcessResult out;

        // RAII
        using jsonPtr = std::unique_ptr<cJSON, decltype(&cJSON_Delete)>;
        jsonPtr root(cJSON_Parse(message.c_str()), &cJSON_Delete);
        if (!root) return out;

        const char* jsonType = cJSON_GetObjectCstr(root.get(), "type");
        if (!jsonType) return out;

        /* clearAudio: flush write buffer on barge-in */
        if (std::strcmp(jsonType, "clearAudio") == 0) {
            clearWriteBuffer();
            return out;  /* out.ok stays FALSE → EVENT_JSON, not EVENT_PLAY */
        }

        if (std::strcmp(jsonType, "streamAudio") != 0) {
            return out; // not ours
        }

        cJSON* jsonData = cJSON_GetObjectItem(root.get(), "data");
        if (!jsonData) {
            push_err(out, m_sessionId, "processMessage - no data in streamAudio");
            return out;
        }

        const char* jsAudioDataType = cJSON_GetObjectCstr(jsonData, "audioDataType");
        if (!jsAudioDataType) jsAudioDataType = "";

        jsonPtr jsonAudio(cJSON_DetachItemFromObject(jsonData, "audioData"), &cJSON_Delete);

        if (!jsonAudio) {
            push_err(out, m_sessionId, "processMessage - streamAudio missing 'audioData' field");
            return out;
        }

        if (!cJSON_IsString(jsonAudio.get()) || !jsonAudio->valuestring) {
            push_err(out, m_sessionId, "processMessage - 'audioData' is not a string (expected base64 string)");
            return out;
        }

        // sampleRate (only meaningful for raw)
        int sampleRate = 0;
        if (cJSON* jsonSampleRate = cJSON_GetObjectItem(jsonData, "sampleRate")) {
            sampleRate = jsonSampleRate->valueint;
        }

        // map file type
        std::string fileType;
        if (std::strcmp(jsAudioDataType, "raw") == 0) {
            switch (sampleRate) {
                case 8000:  fileType = ".r8";  break;
                case 16000: fileType = ".r16"; break;
                case 24000: fileType = ".r24"; break;
                case 32000: fileType = ".r32"; break;
                case 48000: fileType = ".r48"; break;
                case 64000: fileType = ".r64"; break;
                default:
                    push_err(out, m_sessionId, "processMessage - unsupported sample rate: " + std::to_string(sampleRate));
                    return out;
            }
        } else if (std::strcmp(jsAudioDataType, "wav") == 0)  fileType = ".wav";
        else if (std::strcmp(jsAudioDataType, "mp3") == 0)   fileType = ".mp3";
        else if (std::strcmp(jsAudioDataType, "ogg") == 0)   fileType = ".ogg";
        else if (std::strcmp(jsAudioDataType, "pcmu") == 0)  fileType = ".pcmu";
        else if (std::strcmp(jsAudioDataType, "pcma") == 0)  fileType = ".pcma";
        else {
            push_err(out, m_sessionId, "processMessage - unsupported audio type: " + std::string(jsAudioDataType));
            return out;
        }

        // base64 decode
        std::string decoded;
        try {
            decoded = base64_decode(jsonAudio->valuestring);
        } catch (const std::exception& e) {
            push_err(out, m_sessionId, "processMessage - base64 decode error: " + std::string(e.what()));
            return out;
        }

        // reserve file index
        int idx = 0;
        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            idx = m_playFile++;
        }

        char filePath[256];
        switch_snprintf(filePath, sizeof(filePath), "%s%s%s_%d.tmp%s",
                        SWITCH_GLOBAL_dirs.temp_dir, SWITCH_PATH_SEPARATOR,
                        m_sessionId.c_str(), idx, fileType.c_str());

        // write file
        {
            std::ofstream f(filePath, std::ios::binary);
            if (!f.is_open()) {
                push_err(out, m_sessionId, std::string("processMessage - failed to open file for write: ") + filePath);
                return out;
            }
            f.write(decoded.data(), static_cast<std::streamsize>(decoded.size()));
            if (!f.good()) {
                push_err(out, m_sessionId, std::string("processMessage - failed writing file: ") + filePath);
                return out;
            }
        }

        // track file for cleanup
        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            m_Files.insert(filePath);
        }

        cJSON_AddItemToObject(jsonData, "file", cJSON_CreateString(filePath));

        // return rewritten jsonData as string
        char* jsonString = cJSON_PrintUnformatted(jsonData);
        if (!jsonString) {
            push_err(out, m_sessionId, "processMessage - cJSON_PrintUnformatted failed");
            return out;
        }

        out.rewrittenJsonData.assign(jsonString);
        std::free(jsonString);
        out.ok = SWITCH_TRUE;
        return out;
    }

private:
    std::string m_sessionId;
    responseHandler_t m_notify;
    WebSocketClient client;
    bool m_suppress_log;
    const char* m_extra_headers;
    int m_playFile;
    std::unordered_set<std::string> m_Files;
    std::atomic<bool> m_cleanedUp{false};
    std::mutex m_stateMutex;
};


namespace {

    switch_status_t stream_data_init(private_t *tech_pvt, switch_core_session_t *session, char *wsUri,
                                     uint32_t sampling, int desiredSampling, int channels, char *metadata, responseHandler_t responseHandler,
                                     int deflate, int heart_beat, bool suppressLog, int rtp_packets, const char* extra_headers,
                                     const char *tls_cafile, const char *tls_keyfile, const char *tls_certfile,
                                     bool tls_disable_hostname_validation)
    {
        int err; //speex

        switch_memory_pool_t *pool = switch_core_session_get_pool(session);

        memset(tech_pvt, 0, sizeof(private_t));

        strncpy(tech_pvt->sessionId, switch_core_session_get_uuid(session), MAX_SESSION_ID);
        strncpy(tech_pvt->ws_uri, wsUri, MAX_WS_URI);
        tech_pvt->sampling = desiredSampling;
        tech_pvt->responseHandler = responseHandler;
        tech_pvt->rtp_packets = rtp_packets;
        tech_pvt->channels = channels;
        tech_pvt->audio_paused = 0;

        if (metadata) strncpy(tech_pvt->initialMetadata, metadata, MAX_METADATA_LEN);

        //size_t buflen = (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * 1000 / RTP_PERIOD * BUFFERED_SEC);
        const size_t buflen = (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * rtp_packets);

        auto sp = AudioStreamer::create(tech_pvt->sessionId, wsUri, responseHandler, deflate, heart_beat,
                                        suppressLog, extra_headers, tls_cafile, tls_keyfile,
                                        tls_certfile, tls_disable_hostname_validation);

        tech_pvt->pAudioStreamer = new std::shared_ptr<AudioStreamer>(sp);

        switch_mutex_init(&tech_pvt->mutex, SWITCH_MUTEX_NESTED, pool);

        if (switch_buffer_create(pool, &tech_pvt->sbuffer, buflen) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                "%s: Error creating switch buffer.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }

        /* WRITE_REPLACE: create write buffer and mutex */
        switch_mutex_init(&tech_pvt->write_mutex, SWITCH_MUTEX_NESTED, pool);
        if (switch_buffer_create(pool, &tech_pvt->wbuffer, WRITE_BUFFER_SIZE) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                "%s: Error creating write buffer.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }
        tech_pvt->write_active = 0;

        /* WRITE_REPLACE: create write resampler if needed (desiredSampling -> actual channel rate) */
        if (desiredSampling != (int)sampling) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                "(%s) write resampling from %d to %u\n", tech_pvt->sessionId, desiredSampling, sampling);
            tech_pvt->write_resampler = speex_resampler_init(1, desiredSampling, sampling,
                                                              SWITCH_RESAMPLE_QUALITY, &err);
            if (0 != err) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                    "Error initializing write resampler: %s.\n", speex_resampler_strerror(err));
                return SWITCH_STATUS_FALSE;
            }
        }

        /* READ path: create resampler if needed (actual channel rate -> desiredSampling) */
        if (desiredSampling != (int)sampling) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) resampling from %u to %d\n", tech_pvt->sessionId, sampling, desiredSampling);
            tech_pvt->resampler = speex_resampler_init(channels, sampling, desiredSampling, SWITCH_RESAMPLE_QUALITY, &err);
            if (0 != err) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error initializing resampler: %s.\n", speex_resampler_strerror(err));
                return SWITCH_STATUS_FALSE;
            }
        }
        else {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) no resampling needed for this call\n", tech_pvt->sessionId);
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) stream_data_init\n", tech_pvt->sessionId);

        return SWITCH_STATUS_SUCCESS;
    }

    void destroy_tech_pvt(private_t* tech_pvt) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "%s destroy_tech_pvt\n", tech_pvt->sessionId);
        if (tech_pvt->resampler) {
            speex_resampler_destroy(tech_pvt->resampler);
            tech_pvt->resampler = nullptr;
        }
        if (tech_pvt->write_resampler) {
            speex_resampler_destroy(tech_pvt->write_resampler);
            tech_pvt->write_resampler = nullptr;
        }
        if (tech_pvt->mutex) {
            switch_mutex_destroy(tech_pvt->mutex);
            tech_pvt->mutex = nullptr;
        }
        if (tech_pvt->write_mutex) {
            switch_mutex_destroy(tech_pvt->write_mutex);
            tech_pvt->write_mutex = nullptr;
        }
    }

}

extern "C" {
    int validate_ws_uri(const char* url, char* wsUri) {
        const char* scheme = nullptr;
        const char* hostStart = nullptr;
        const char* hostEnd = nullptr;
        const char* portStart = nullptr;

        // Check scheme
        if (strncmp(url, "ws://", 5) == 0) {
            scheme = "ws";
            hostStart = url + 5;
        } else if (strncmp(url, "wss://", 6) == 0) {
            scheme = "wss";
            hostStart = url + 6;
        } else {
            return 0;
        }

        // Find host end or port start
        hostEnd = hostStart;
        while (*hostEnd && *hostEnd != ':' && *hostEnd != '/') {
            if (!std::isalnum(*hostEnd) && *hostEnd != '-' && *hostEnd != '.') {
                return 0;
            }
            ++hostEnd;
        }

        // Check if host is empty
        if (hostStart == hostEnd) {
            return 0;
        }

        // Check for port
        if (*hostEnd == ':') {
            portStart = hostEnd + 1;
            while (*portStart && *portStart != '/') {
                if (!std::isdigit(*portStart)) {
                    return 0;
                }
                ++portStart;
            }
        }

        // Copy valid URI to wsUri
        std::strncpy(wsUri, url, MAX_WS_URI);
        return 1;
    }

    switch_status_t is_valid_utf8(const char *str) {
        switch_status_t status = SWITCH_STATUS_FALSE;
        while (*str) {
            if ((*str & 0x80) == 0x00) {
                // 1-byte character
                str++;
            } else if ((*str & 0xE0) == 0xC0) {
                // 2-byte character
                if ((str[1] & 0xC0) != 0x80) {
                    return status;
                }
                str += 2;
            } else if ((*str & 0xF0) == 0xE0) {
                // 3-byte character
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80) {
                    return status;
                }
                str += 3;
            } else if ((*str & 0xF8) == 0xF0) {
                // 4-byte character
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80 || (str[3] & 0xC0) != 0x80) {
                    return status;
                }
                str += 4;
            } else {
                // invalid character
                return status;
            }
        }
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_send_text(switch_core_session_t *session, char* text) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_send_text failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        std::shared_ptr<AudioStreamer> streamer;

        switch_mutex_lock(tech_pvt->mutex);

        if (tech_pvt->pAudioStreamer) {
            auto sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap; // copy shared_ptr
            }
        }

        switch_mutex_unlock(tech_pvt->mutex);

        if (streamer) {
            streamer->writeText(text);
            return SWITCH_STATUS_SUCCESS;
        }

        return SWITCH_STATUS_FALSE;
    }

    switch_status_t stream_session_pauseresume(switch_core_session_t *session, int pause) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_pauseresume failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        switch_core_media_bug_flush(bug);
        tech_pvt->audio_paused = pause;
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_init(switch_core_session_t *session,
                                        responseHandler_t responseHandler,
                                        uint32_t samples_per_second,
                                        char *wsUri,
                                        int sampling,
                                        int channels,
                                        char* metadata,
                                        void **ppUserData)
    {
        int deflate, heart_beat;
        bool suppressLog = false;
        const char* buffer_size;
        const char* extra_headers;
        int rtp_packets = 1; //20ms burst
        const char* tls_cafile = NULL;;
        const char* tls_keyfile = NULL;;
        const char* tls_certfile = NULL;;
        bool tls_disable_hostname_validation = false;

        switch_channel_t *channel = switch_core_session_get_channel(session);

        if (switch_channel_var_true(channel, "STREAM_MESSAGE_DEFLATE")) {
            deflate = 1;
        }

        if (switch_channel_var_true(channel, "STREAM_SUPPRESS_LOG")) {
            suppressLog = true;
        }

        tls_cafile = switch_channel_get_variable(channel, "STREAM_TLS_CA_FILE");
        tls_keyfile = switch_channel_get_variable(channel, "STREAM_TLS_KEY_FILE");
        tls_certfile = switch_channel_get_variable(channel, "STREAM_TLS_CERT_FILE");

        if (switch_channel_var_true(channel, "STREAM_TLS_DISABLE_HOSTNAME_VALIDATION")) {
            tls_disable_hostname_validation = true;
        }

        const char* heartBeat = switch_channel_get_variable(channel, "STREAM_HEART_BEAT");
        if (heartBeat) {
            char *endptr;
            long value = strtol(heartBeat, &endptr, 10);
            if (*endptr == '\0' && value <= INT_MAX && value >= INT_MIN) {
                heart_beat = (int) value;
            }
        }

        if ((buffer_size = switch_channel_get_variable(channel, "STREAM_BUFFER_SIZE"))) {
            int bSize = atoi(buffer_size);
            if(bSize % 20 != 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "%s: Buffer size of %s is not a multiple of 20ms. Using default 20ms.\n",
                                  switch_channel_get_name(channel), buffer_size);
            } else if(bSize >= 20){
                rtp_packets = bSize/20;
            }
        }

        extra_headers = switch_channel_get_variable(channel, "STREAM_EXTRA_HEADERS");

        // allocate per-session tech_pvt
        auto* tech_pvt = (private_t *) switch_core_session_alloc(session, sizeof(private_t));

        if (!tech_pvt) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "error allocating memory!\n");
            return SWITCH_STATUS_FALSE;
        }
        if (SWITCH_STATUS_SUCCESS != stream_data_init(tech_pvt, session, wsUri, samples_per_second, sampling, channels,
                                                        metadata, responseHandler, deflate, heart_beat, suppressLog, rtp_packets,
                                                        extra_headers, tls_cafile, tls_keyfile, tls_certfile, tls_disable_hostname_validation)) {
            destroy_tech_pvt(tech_pvt);
            return SWITCH_STATUS_FALSE;
        }

        *ppUserData = tech_pvt;

        return SWITCH_STATUS_SUCCESS;
    }

    switch_bool_t stream_frame(switch_media_bug_t *bug) {
        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) return SWITCH_TRUE;
        if (tech_pvt->audio_paused || tech_pvt->cleanup_started) return SWITCH_TRUE;

        std::shared_ptr<AudioStreamer> streamer;
        std::vector<std::vector<uint8_t>> pending_send;

        if (switch_mutex_trylock(tech_pvt->mutex) != SWITCH_STATUS_SUCCESS) {
            return SWITCH_TRUE;
        }

        if (!tech_pvt->pAudioStreamer) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        auto sp_ptr = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
        if (!sp_ptr || !(*sp_ptr)) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        streamer = *sp_ptr;

        auto *resampler = tech_pvt->resampler;
        const int channels = tech_pvt->channels;
        const int rtp_packets = tech_pvt->rtp_packets;

        if (nullptr == resampler) {

            uint8_t data_buf[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data_buf;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if (!frame.datalen) {
                    continue;
                }

                if (rtp_packets == 1) {
                    pending_send.emplace_back((uint8_t*)frame.data, (uint8_t*)frame.data + frame.datalen);
                    continue;
                }

                size_t freespace = switch_buffer_freespace(tech_pvt->sbuffer);

                if (freespace >= frame.datalen) {
                    switch_buffer_write(tech_pvt->sbuffer, static_cast<uint8_t *>(frame.data), frame.datalen);
                }

                if (switch_buffer_freespace(tech_pvt->sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(tech_pvt->sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(tech_pvt->sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(tech_pvt->sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }

        } else {

            uint8_t data[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if(!frame.datalen) {
                    continue;
                }

                const size_t freespace = switch_buffer_freespace(tech_pvt->sbuffer);
                spx_uint32_t in_len = frame.samples;
                spx_uint32_t out_len = (freespace / (tech_pvt->channels * sizeof(spx_int16_t)));

                if(out_len == 0) {
                    if(freespace == 0) {
                        switch_size_t inuse = switch_buffer_inuse(tech_pvt->sbuffer);
                        if (inuse > 0) {
                            std::vector<uint8_t> tmp(inuse);
                            switch_buffer_read(tech_pvt->sbuffer, tmp.data(), inuse);
                            switch_buffer_zero(tech_pvt->sbuffer);
                            pending_send.emplace_back(std::move(tmp));
                        }
                    }
                    continue;
                }

                std::vector<spx_int16_t> out;
                out.resize((size_t)out_len * (size_t)channels);

                if(channels == 1) {
                    speex_resampler_process_int(resampler,
                                    0,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    out.data(),
                                    &out_len);
                } else {
                    speex_resampler_process_interleaved_int(resampler,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    out.data(),
                                    &out_len);
                }

                if(out_len > 0) {
                    const size_t bytes_written = (size_t)out_len * (size_t)channels * sizeof(spx_int16_t);

                    if (rtp_packets == 1) { //20ms packet
                        const uint8_t* p = (const uint8_t*)out.data();
                        pending_send.emplace_back(p, p + bytes_written);
                        continue;
                    }

                    if (bytes_written <= switch_buffer_freespace(tech_pvt->sbuffer)) {
                        switch_buffer_write(tech_pvt->sbuffer, (const uint8_t *)out.data(), bytes_written);
                    }
                }

                if (switch_buffer_freespace(tech_pvt->sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(tech_pvt->sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(tech_pvt->sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(tech_pvt->sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }
        }

        switch_mutex_unlock(tech_pvt->mutex);

        if (!streamer || !streamer->isConnected()) return SWITCH_TRUE;

        for (auto &chunk : pending_send) {
            if (!chunk.empty()) {
                streamer->writeBinary(chunk.data(), chunk.size());
            }
        }

        return SWITCH_TRUE;
    }

    /**
     * WRITE_REPLACE callback: inject audio from the write buffer into the
     * channel's outgoing (write) stream.
     *
     * Called every 20ms by FreeSWITCH with the current write frame.
     * If the write buffer has audio, we replace the frame data.
     * If the buffer is empty and write was never activated, the original
     * frame passes through unchanged.
     * If the buffer is empty but write was activated (bot paused between
     * utterances), we inject silence to prevent stale audio.
     */
    switch_bool_t stream_frame_write(switch_media_bug_t *bug, void *user_data) {
        auto* tech_pvt = (private_t*) user_data;
        if (!tech_pvt || tech_pvt->cleanup_started) return SWITCH_TRUE;
        if (!tech_pvt->wbuffer) return SWITCH_TRUE;

        switch_frame_t *write_frame = switch_core_media_bug_get_write_replace_frame(bug);

        if (!write_frame || !write_frame->data || write_frame->datalen == 0) {
            return SWITCH_TRUE;
        }

        const size_t frame_bytes = write_frame->datalen;

        static int write_log_counter = 0;
        switch_mutex_lock(tech_pvt->write_mutex);
        switch_size_t available = switch_buffer_inuse(tech_pvt->wbuffer);
        int was_active = tech_pvt->write_active;

        if (++write_log_counter % 250 == 1) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                "stream_frame_write: frame_bytes=%zu, available=%zu, was_active=%d, has_resampler=%d, frame_rate=%u\n",
                frame_bytes, (size_t)available, was_active,
                tech_pvt->write_resampler ? 1 : 0,
                (unsigned)write_frame->rate);
        }

        if (tech_pvt->write_resampler && available > 0) {
            /*
             * Resample from desired rate (e.g. 16kHz) to channel rate (e.g. 8kHz).
             * Read enough input samples to produce one frame of output.
             *
             * For 8kHz output, 20ms frame = 160 samples = 320 bytes.
             * For 16kHz input, we need 320 input samples = 640 bytes.
             */
            spx_uint32_t out_samples = write_frame->datalen / sizeof(spx_int16_t);
            /* Compute input samples needed from resampler ratio.
             * write_resampler: desiredSampling (e.g. 16k) -> channel rate (e.g. 8k).
             * For out_samples output, we need in_samples = out_samples * (16k/8k).
             */
            spx_uint32_t ratio_num, ratio_den;
            speex_resampler_get_ratio(tech_pvt->write_resampler, &ratio_num, &ratio_den);
            spx_uint32_t in_samples_needed = (spx_uint32_t)((uint64_t)out_samples * ratio_num / ratio_den + 1);
            size_t in_bytes_needed = in_samples_needed * sizeof(spx_int16_t);

            if (available >= in_bytes_needed) {
                /* Read input samples from write buffer */
                std::vector<spx_int16_t> in_buf(in_samples_needed);
                switch_buffer_read(tech_pvt->wbuffer, in_buf.data(), in_bytes_needed);

                spx_uint32_t in_len = in_samples_needed;
                spx_uint32_t out_len = out_samples;

                speex_resampler_process_int(tech_pvt->write_resampler,
                                            0,
                                            in_buf.data(), &in_len,
                                            (spx_int16_t*)write_frame->data, &out_len);

                write_frame->datalen = out_len * sizeof(spx_int16_t);
                write_frame->samples = out_len;
            } else if (available > 0) {
                /* Partial data: read what we have, pad with silence */
                size_t have = available;
                spx_uint32_t in_len = (spx_uint32_t)(have / sizeof(spx_int16_t));
                spx_uint32_t out_len = out_samples;

                std::vector<spx_int16_t> in_buf(in_len);
                switch_buffer_read(tech_pvt->wbuffer, in_buf.data(), have);

                speex_resampler_process_int(tech_pvt->write_resampler,
                                            0,
                                            in_buf.data(), &in_len,
                                            (spx_int16_t*)write_frame->data, &out_len);

                /* Zero-pad remaining output */
                if (out_len < out_samples) {
                    memset((spx_int16_t*)write_frame->data + out_len, 0,
                           (out_samples - out_len) * sizeof(spx_int16_t));
                }
                write_frame->datalen = out_samples * sizeof(spx_int16_t);
                write_frame->samples = out_samples;
            } else if (was_active) {
                /* Buffer empty but was active: inject silence */
                memset(write_frame->data, 0, frame_bytes);
            } else {
                /* Never activated: pass original frame through */
                switch_mutex_unlock(tech_pvt->write_mutex);
                return SWITCH_TRUE;
            }
        } else if (available >= frame_bytes) {
            /* No resampling needed: direct copy from buffer to frame */
            switch_buffer_read(tech_pvt->wbuffer, write_frame->data, frame_bytes);
        } else if (available > 0) {
            /* Partial: fill what we have, pad rest with silence */
            switch_buffer_read(tech_pvt->wbuffer, write_frame->data, available);
            memset((uint8_t*)write_frame->data + available, 0, frame_bytes - available);
        } else if (was_active) {
            /* Buffer empty but was active: inject silence */
            memset(write_frame->data, 0, frame_bytes);
        } else {
            /* Never activated: pass original frame through */
            switch_mutex_unlock(tech_pvt->write_mutex);
            return SWITCH_TRUE;
        }

        switch_mutex_unlock(tech_pvt->write_mutex);

        switch_core_media_bug_set_write_replace_frame(bug, write_frame);
        return SWITCH_TRUE;
    }

    switch_status_t stream_session_cleanup(switch_core_session_t *session, char* text, int channelIsClosing) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if(bug)
        {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            char sessionId[MAX_SESSION_ID];
            strcpy(sessionId, tech_pvt->sessionId);

            std::shared_ptr<AudioStreamer>* sp_wrap = nullptr;
            std::shared_ptr<AudioStreamer> streamer;

            switch_mutex_lock(tech_pvt->mutex);

            if (tech_pvt->cleanup_started) {
                switch_mutex_unlock(tech_pvt->mutex);
                return SWITCH_STATUS_SUCCESS;
            }
            tech_pvt->cleanup_started = 1;

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) stream_session_cleanup\n", sessionId);

            switch_channel_set_private(channel, MY_BUG_NAME, nullptr);

            sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            tech_pvt->pAudioStreamer = nullptr;

            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap;
            }

            switch_mutex_unlock(tech_pvt->mutex);

            if (!channelIsClosing) {
                switch_core_media_bug_remove(session, &bug);
            }

            if (sp_wrap) {
                delete sp_wrap;
                sp_wrap = nullptr;
            }

            if(streamer) {
                streamer->deleteFiles();
                if (text) streamer->writeText(text);

                streamer->markCleanedUp();
                streamer->disconnect();
            }

            destroy_tech_pvt(tech_pvt);

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "(%s) stream_session_cleanup: connection closed\n", sessionId);
            return SWITCH_STATUS_SUCCESS;
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "stream_session_cleanup: no bug - websocket connection already closed\n");
        return SWITCH_STATUS_FALSE;
    }
}
