package org.techtown.unsjedis

import android.annotation.SuppressLint
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import androidx.annotation.RequiresApi
import androidx.core.app.NotificationCompat
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import kotlin.concurrent.timer

/**
 * 레디스 서비스
 */
class JedisService : Service() {
    companion object {
        private const val TAG: String = "JedisService"
    }

    data class RedisInfo(
        val client: Jedis,
        val channel: String,
        var connection: JedisPubSub? = null,
        var isConnected: Boolean = false,
    )

    private val connectionHandler by lazy { Handler(Looper.getMainLooper()) }
    private val handler by lazy { Handler(Looper.getMainLooper()) }
    private var timer: Timer? = null

    // thread-safe (여러 스레드에서 동시에 사용 가능한 변수들)
    private val clients by lazy { ConcurrentLinkedDeque<RedisInfo>() }
    private val isConnecting = AtomicBoolean(false)

    private var channelId = ""
    private var host = ""
    private var port = 0

    override fun onDestroy() {
        super.onDestroy()
        disconnectRedis()
    }

    @RequiresApi(Build.VERSION_CODES.O)
    override fun onCreate() {
        super.onCreate()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) initNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        intent?.let { handleCommand(it) }
        return super.onStartCommand(intent, flags, startId)
    }

    /**
     * 알림 채널 초기화
     */
    @RequiresApi(Build.VERSION_CODES.O)
    private fun initNotificationChannel() {
        val channelId = "redis_channel"
        val notificationChannel =
            NotificationChannel(channelId, "Redis Channel", NotificationManager.IMPORTANCE_LOW)
        val notificationManager =
            applicationContext.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        notificationManager.createNotificationChannel(notificationChannel)
        val notification = NotificationCompat.Builder(this, channelId)
            .setContentTitle("Redis")
            .setContentText("$TAG started.").build()
        startForeground(1, notification)
    }

    private fun handleCommand(intent: Intent) {
        val command = intent.getStringExtra(Extras.COMMAND)
        AppData.debug(TAG, "command in RedisService : $command")

        command?.apply {
            when (this) {
                Extras.CONNECT -> {
                    host = intent.getStringExtra(Extras.REDIS_HOST).toString()
                    port = intent.getIntExtra(Extras.REDIS_PORT, 0)
                    channelId = intent.getStringExtra(Extras.MY_CHANNEL).toString()
                    connectionHandler.removeMessages(0)
                    connectionHandler.postDelayed(::connectRedis, 500)
                }

                Extras.DISCONNECT -> {
                    connectionHandler.removeMessages(0)
                    connectionHandler.postDelayed(::disconnectRedis, 500)
                }

                Extras.SEND -> {
                    intent.getStringExtra(Extras.CHANNEL)?.let {
                        val data = intent.getStringExtra(Extras.DATA) ?: "전달할 데이터 없음"
                        sendData(it, data)
                    }
                }
            }
        }
    }

    private fun connectRedis() {
        AppData.debug(TAG, "connectRedis called.")

        thread {
            // 다른 channel 의 기존 연결이 있다면 끊고 다시 연결 시도 (clients 에 값이 있다면 언제나 단 하나)
            clients.forEach {
                if (it.channel != channelId) {
                    disconnectRedis(true)
                    return@thread
                }
            }
            // 이미 연결되어있다면 리턴, 없다면 true 로 변환하고 넘어간다.
            if (isConnecting.getAndSet(true)) {
                sendData(channelId, "already connected. $channelId - $host:$port")
                return@thread
            }
            // 이 부분에서 size 는 언제나 0 이어야 한다.
            AppData.error(TAG, "clients.size: ${clients.size}")

            Jedis(host, port).let { clients.add(RedisInfo(it, channelId)) }
            subscribeChannel()
        }
    }

    private fun disconnectRedis(isReconnect: Boolean = false) {
        AppData.debug(TAG, "disconnectRedis called.")
        thread {
            timer?.cancel()
            isConnecting.set(false)
            clients.forEach {
                it.client.close()
                it.isConnected = false
                broadcastToActivity(it.channel, "${it.channel} unsubscribed")
            }
            clients.clear()
            if (isReconnect) connectRedis()
        }
    }

    // CHECK_INTERVAL 마다 나 자신에게 메시지를 보낸다.
    private fun checkConnection() {
        thread {
            timer?.cancel()
            timer = timer(period = Extras.CHECK_INTERVAL) {
                Jedis(host, port).use {
                    try {
                        @SuppressLint("SimpleDateFormat")
                        val now = SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Date())
                        it.publish(channelId, "check redis connection $now")
                    } catch (e: Exception) {
                        broadcastToActivity(Extras.UNKNOWN, "fail to reconnect")
                        e.printStackTrace()

                        broadcastToActivity(Extras.UNKNOWN, "try to reconnect")
                        handler.postDelayed(::connectRedis, 30 * 1000L)
                    }
                }
            }
        }
    }

    private fun subscribeChannel() {
        thread {
            clients.forEach {
                it.connection = object : JedisPubSub() {
                    override fun onMessage(channel: String, data: String) {
                        AppData.debug(TAG, "message received in RedisService - channel : ${channel}, data : $data")
                        onMessageReceived(channel, data)
                    }

                    override fun onSubscribe(channel: String, subscribedChannels: Int) {
                        broadcastToActivity(channel, "$channel subscribed")
                        sendData(channel, "successfully connected. $channel - $host:$port")
                        handler.postDelayed(::checkConnection, 1000)
                    }
                }

                try {
                    it.client.subscribe(it.connection, channelId)
                } catch (e: Exception) {
                    if (!isConnecting.get()) return@thread

                    timer?.cancel()
                    clients.forEach { client ->
                        client.client.close()
                    }
                    clients.clear()
                    isConnecting.set(false)
                    broadcastToActivity(Extras.UNKNOWN, "fail to connect")

                    broadcastToActivity(Extras.UNKNOWN, "try to reconnect")
                    handler.postDelayed(::connectRedis, 30 * 1000L)
                }
            }
        }
    }

    private fun onMessageReceived(channel: String, data: String) {
        AppData.debug(TAG, "onMessageReceived called in RedisService.")
        broadcastToActivity(channel, data)
    }

    private fun broadcastToActivity(channel: String, data: String) = with(Intent(AppData.ACTION_REMOTE_DATA)) {
        AppData.error(TAG, "broadcastToActivity called. channel : $channel, data : $data")
        putExtra(Extras.COMMAND, "REDIS")
        putExtra("channel", channel)
        putExtra("data", data)
        sendBroadcast(this)
    }

    private fun sendData(channel: String, data: String) {
        AppData.debug(TAG, "sendData called. channel: $channel, data: $data")
        thread {
            Jedis(host, port).use {
                try {
                    it.publish(channel, data)
                } catch (e: Exception) {
                    AppData.error(TAG, "Error publishing message to channel $channel", e)
                }
            }
        }
    }

    override fun onBind(intent: Intent): IBinder =
        throw UnsupportedOperationException("Not yet implemented")
}