package org.techtown.unsjedis

import android.annotation.SuppressLint
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.Build
import android.os.Bundle
import android.text.method.ScrollingMovementMethod
import android.view.inputmethod.InputMethodManager
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import org.techtown.unsjedis.databinding.ActivityMainBinding
import java.text.SimpleDateFormat
import java.util.*

class MainActivity : AppCompatActivity() {
    private val binding by lazy { ActivityMainBinding.inflate(layoutInflater) }
    private val tag: String = javaClass.simpleName

    private lateinit var receiver: BroadcastReceiver
    private lateinit var filter: IntentFilter

    private var channelList = arrayOf("test01", "test02")

    override fun onStart() {
        super.onStart()
        val method = Thread.currentThread().stackTrace[2].methodName
        AppData.debug(tag, "$method called.")
        ContextCompat.registerReceiver(baseContext, receiver, filter, ContextCompat.RECEIVER_NOT_EXPORTED)
    }

    override fun onStop() {
        val method = Thread.currentThread().stackTrace[2].methodName
        AppData.debug(tag, "$method called.")
        unregisterReceiver(receiver)
        super.onStop()
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(binding.root)

        initReceiver()

        initView()
    }

    private fun initView() = with(binding) {
        val method = Thread.currentThread().stackTrace[2].methodName
        AppData.debug(tag, "$method called. SDK: ${Build.VERSION.SDK_INT}")

        resultText.movementMethod = ScrollingMovementMethod()

        connect1Button.setOnClickListener {
            binding.statusText.text = getString(R.string.connecting)
            connect(channelList.first())
            connect1Button.isEnabled = false
            connect2Button.isEnabled = true
        }

        connect2Button.setOnClickListener {
            binding.statusText.text = getString(R.string.connecting)
            connect(channelList.last())
            connect1Button.isEnabled = true
            connect2Button.isEnabled = false
        }

        disconnectButton.setOnClickListener {
            connect1Button.isEnabled = true
            connect2Button.isEnabled = true
            disconnect()
        }

        sendButton.setOnClickListener {
            binding.chatInput.apply {
                when {
                    connect1Button.isEnabled && connect2Button.isEnabled ->
                        AppData.showToast(this@MainActivity, "현재 연결이 없음")

                    else -> {
                        val channel = if (connect1Button.isEnabled) channelList.first() else channelList.last()
                        sendData(channel, this.text.toString().trim())
                    }
                }
                val imm = getSystemService(Context.INPUT_METHOD_SERVICE) as InputMethodManager
                imm.hideSoftInputFromWindow(this.windowToken, 0)
                this.setText("")
            }
        }
    }

    // 리시버 초기화
    private fun initReceiver() {
        val method = Thread.currentThread().stackTrace[2].methodName
        AppData.debug(tag, "$method called.")
        filter = IntentFilter()
        filter.addAction(AppData.ACTION_REMOTE_DATA)
        receiver = object : BroadcastReceiver() {
            override fun onReceive(context: Context, intent: Intent) = setReceivedData(intent)
        }
    }

    fun setReceivedData(intent: Intent) {
        val command = intent.getStringExtra(Extras.COMMAND)
        val channel = intent.getStringExtra(Extras.CHANNEL)
        val data = intent.getStringExtra(Extras.DATA)
        AppData.debug(tag, "$command : $channel - $data")
        printLog("$command : $channel - $data")
        data?.apply {
            when {
                startsWith("check redis connection") -> return
                startsWith("already connected") or startsWith("successfully connected") -> {
                    binding.statusText.text = getString(R.string.connected)
                    binding.idText.text = channel
                }

                endsWith("unsubscribed") -> {
                    binding.statusText.text = getString(R.string.disconnect)
                    binding.idText.text = Extras.UNKNOWN
                }

                equals("fail to connect") ->
                    binding.statusText.text = getString(R.string.fail_to_connect)

                equals("fail to reconnect") ->
                    binding.statusText.text = getString(R.string.fail_to_reconnect)

                else -> setData(this)
            }
        }
    }

    private fun setData(data: String) {
        val method = Thread.currentThread().stackTrace[2].methodName
        AppData.debug(tag, "$method called. data: $data")
        // TODO: 받은 메시지 처리 로직 작성
    }

    private fun connect(channel: String) {
        val host = AppData.REDIS_HOST
        val port = AppData.REDIS_PORT
        AppData.debug(tag, "connect called : $channel, $host:$port")
        binding.idText.text = channel

        Intent(this, JedisService::class.java).also {
            it.putExtra(Extras.COMMAND, Extras.CONNECT)
            it.putExtra(Extras.REDIS_HOST, host)
            it.putExtra(Extras.REDIS_PORT, port)
            it.putExtra(Extras.MY_CHANNEL, channel)
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O)
                startService(it)
            else startForegroundService(it)
        }
    }

    private fun disconnect() {
        AppData.debug(tag, "disconnect called")

        Intent(this, JedisService::class.java).also {
            it.putExtra(Extras.COMMAND, Extras.DISCONNECT)
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O)
                startService(it)
            else startForegroundService(it)
        }
    }

    private fun sendData(channel: String, data: String) = with(Intent(this, JedisService::class.java)) {
        printLog("to $channel : $data")
        putExtra(Extras.COMMAND, Extras.SEND)
        putExtra(Extras.CHANNEL, channel)
        putExtra(Extras.DATA, data)
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O)
            startService(this)
        else startForegroundService(this)
    }

    private fun printLog(message: String) = runOnUiThread {
        @SuppressLint("SimpleDateFormat")
        val now = SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Date())
        val log = "[$now] $message"
        if (AppData.logList.size > 1000) AppData.logList.removeAt(1)
        AppData.logList.add(log)
        val sb = StringBuilder()
        AppData.logList.forEach { sb.appendLine(it) }
        binding.resultText.text = sb
        moveToBottom(binding.resultText)
    }

    private fun moveToBottom(textView: TextView) = textView.post {
        val scrollAmount = try {
            textView.layout.getLineTop(textView.lineCount) - textView.height
        } catch (_: NullPointerException) {
            0
        }
        if (scrollAmount > 0) textView.scrollTo(0, scrollAmount)
        else textView.scrollTo(0, 0)
    }
}