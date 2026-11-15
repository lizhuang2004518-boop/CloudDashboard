// ============================================================================
//  mainwindow.cpp —— 云端考勤管理与数据大屏（实现）
//
//  目录：
//    第 1 节  MQTT 3.1.1 报文编码工具
//    第 2 节  MqttWsClient：QWebSocket + MQTT 协议
//    第 3 节  MainWindow 构造 / 界面 / QSS
//    第 4 节  业务槽函数（录入 / 删除 / 收打卡）
//    第 5 节  OpenCV 模型加载与特征提取
//    第 6 节  表格与图表更新
// ============================================================================

#include "mainwindow.h"

#include <QAbstractSocket>
#include <algorithm>
#include <QApplication>
#include <QBrush>
#include <QCoreApplication>
#include <QDateTime>
#include <QDebug>
#include <QDialog>
#include <QDialogButtonBox>
#include <QDir>
#include <QFile>
#include <QFileDialog>
#include <QFileInfo>
#include <QFormLayout>
#include <QFrame>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QInputDialog>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonParseError>
#include <QLabel>
#include <QLineEdit>
#include <QList>
#include <QMessageBox>
#include <QNetworkRequest>
#include <QPainter>
#include <QPen>
#include <QPixmap>
#include <QPushButton>
#include <QSplitter>
#include <QStatusBar>
#include <QStringList>
#include <QTableWidget>
#include <QTableWidgetItem>
#include <QTime>
#include <QTimer>
#include <QUuid>
#include <QVBoxLayout>
#include <QWebSocket>

#include <QtCharts/QChart>
#include <QtCharts/QChartView>
#include <QtCharts/QDateTimeAxis>
#include <QtCharts/QLegend>
#include <QtCharts/QLineSeries>
#include <QtCharts/QPieSeries>
#include <QtCharts/QPieSlice>
#include <QtCharts/QValueAxis>

#include <opencv2/opencv.hpp>

QT_CHARTS_USE_NAMESPACE

// ============================================================================
//  第 1 节：MQTT 3.1.1 报文编码工具
//
//  固定头 = 1 字节（报文类型<<4 | 标志） + 1~4 字节「剩余长度」
//  剩余长度是变长编码：每字节低 7 位有效，最高位表示「后面还有」
//  字符串 = 2 字节大端长度 + UTF-8 内容
// ============================================================================
namespace {

// 变长编码剩余长度
QByteArray encodeRemainingLength(int length)
{
    QByteArray out;
    do {
        quint8 digit = quint8(length % 128);
        length /= 128;
        if (length > 0)
            digit |= 0x80;              // 还有后续字节
        out.append(char(digit));
    } while (length > 0);
    return out;
}

// MQTT 字符串（主题 / 客户端 ID 都是这个格式）
QByteArray encodeMqttString(const QString &text)
{
    const QByteArray utf8 = text.toUtf8();
    QByteArray out;
    out.append(char((utf8.size() >> 8) & 0xFF));
    out.append(char(utf8.size() & 0xFF));
    out.append(utf8);
    return out;
}

// 拼完整报文
QByteArray makePacket(quint8 header, const QByteArray &body)
{
    QByteArray packet;
    packet.append(char(header));
    packet.append(encodeRemainingLength(body.size()));
    packet.append(body);
    return packet;
}

// 解析剩余长度。偏移量进来指向长度字段首字节，出去指向报文体首字节。
// 返回 0 成功 / -1 数据不全 / -2 非法（超过 4 字节）
int readRemainingLength(const QByteArray &data, int &offset, int &length)
{
    int multiplier = 1;
    length = 0;
    int pos = offset;

    for (int i = 0; i < 4; ++i) {
        if (pos >= data.size())
            return -1;                  // 半包，等下一帧
        const quint8 byte = quint8(data.at(pos++));
        length += int(byte & 0x7F) * multiplier;
        multiplier *= 128;
        if ((byte & 0x80) == 0) {
            offset = pos;
            return 0;
        }
    }
    return -2;
}

} // namespace

// ============================================================================
//  第 2 节：MqttWsClient
// ============================================================================
MqttWsClient::MqttWsClient(QObject *parent)
    : QObject(parent)
    , m_ws(new QWebSocket(QString(), QWebSocketProtocol::VersionLatest, this))
    , m_pingTimer(new QTimer(this))
{
    connect(m_ws, &QWebSocket::connected, this, &MqttWsClient::onSocketConnected);
    connect(m_ws, &QWebSocket::disconnected, this, &MqttWsClient::onSocketDisconnected);
    connect(m_ws, &QWebSocket::binaryMessageReceived, this, &MqttWsClient::onBinaryMessage);

    // Qt 5.14 的错误信号叫 error()（5.15 起新增 errorOccurred）。
    // 用 QOverload 明确指向带参数的那个信号，避免歧义。
    connect(m_ws, QOverload<QAbstractSocket::SocketError>::of(&QWebSocket::error),
            this, &MqttWsClient::onSocketError);

    m_pingTimer->setInterval(30000);    // CONNECT 里 keepalive=60s，30s 心跳一次
    connect(m_pingTimer, &QTimer::timeout, this, &MqttWsClient::onPingTimer);
}

MqttWsClient::~MqttWsClient()
{
    close();
}

bool MqttWsClient::isConnecting() const
{
    const QAbstractSocket::SocketState state = m_ws->state();
    return state == QAbstractSocket::ConnectingState
        || state == QAbstractSocket::HostLookupState;
}

void MqttWsClient::open(const QUrl &url, const QString &clientId)
{
    close();
    m_clientId = clientId;

    // ★ MQTT over WebSocket 规范（MQTT 3.1.1 第 6 章）要求握手时带
    //   Sec-WebSocket-Protocol: mqtt。Qt5 的 QWebSocket 没有 setSubprotocols()，
    //   但 open(QNetworkRequest) 会把 request 里的原始头逐条写进握手报文
    //   （Qt 源码 qwebsocket_p.cpp 用 m_request.rawHeaderList() 拼握手），
    //   所以 setRawHeader 能达到同样效果。
    QNetworkRequest request(url);
    request.setRawHeader("Sec-WebSocket-Protocol", "mqtt");
    m_ws->open(request);
}

void MqttWsClient::close()
{
    m_pingTimer->stop();

    if (m_ws->state() != QAbstractSocket::UnconnectedState) {
        if (m_connected)
            sendPacket(makePacket(0xE0, QByteArray()));   // DISCONNECT
        m_ws->close();
    }
    m_connected = false;
    m_rxBuffer.clear();
}

void MqttWsClient::onSocketConnected()
{
    // WebSocket 握手完成 -> 发 MQTT CONNECT，等 CONNACK 才算真正连上 broker
    sendPacket(buildConnectPacket(m_clientId));
}

void MqttWsClient::onSocketDisconnected()
{
    m_pingTimer->stop();
    if (m_connected) {
        m_connected = false;
        emit disconnected();
    }
}

void MqttWsClient::onSocketError(QAbstractSocket::SocketError error)
{
    emit errorOccurred(QStringLiteral("WebSocket 错误（%1）：%2")
                           .arg(int(error))
                           .arg(m_ws->errorString()));
}

void MqttWsClient::onPingTimer()
{
    sendPacket(makePacket(0xC0, QByteArray()));   // PINGREQ
}

void MqttWsClient::sendPacket(const QByteArray &packet)
{
    if (m_ws->state() != QAbstractSocket::ConnectedState)
        return;
    // MQTT over WebSocket 必须用二进制帧
    m_ws->sendBinaryMessage(packet);
}

void MqttWsClient::sendPubAck(quint16 packetId)
{
    // QoS1 的 PUBLISH 必须回 PUBACK，否则 broker 会认为没收到而重发
    QByteArray body;
    body.append(char((packetId >> 8) & 0xFF));
    body.append(char(packetId & 0xFF));
    sendPacket(makePacket(0x40, body));      // 0x40 = PUBACK
}

QByteArray MqttWsClient::buildConnectPacket(const QString &clientId) const
{
    QByteArray body;
    body.append(encodeMqttString(QStringLiteral("MQTT")));  // 协议名
    body.append(char(4));                                   // 协议级别 4 = MQTT 3.1.1
    body.append(char(0x02));                                // 连接标志：clean session
    body.append(char(0));                                   // keepalive 高字节
    body.append(char(60));                                  // keepalive = 60 秒
    body.append(encodeMqttString(clientId));                // 客户端 ID
    return makePacket(0x10, body);                          // CONNECT
}

QByteArray MqttWsClient::buildSubscribePacket(quint16 packetId, const QString &topic, int qos) const
{
    QByteArray body;
    body.append(char((packetId >> 8) & 0xFF));              // 报文标识符（不能为 0）
    body.append(char(packetId & 0xFF));
    body.append(encodeMqttString(topic));                   // 主题过滤器
    body.append(char(qos & 0x03));                          // 请求的 QoS
    return makePacket(0x82, body);                          // SUBSCRIBE + flags 0010
}

QByteArray MqttWsClient::buildPublishPacket(const QString &topic, const QByteArray &payload) const
{
    QByteArray body;
    body.append(encodeMqttString(topic));                   // 主题
    body.append(payload);                                   // 载荷（QoS0 无报文标识符）
    return makePacket(0x30, body);                          // PUBLISH, QoS0, 不保留
}

void MqttWsClient::subscribe(const QString &topic, int qos)
{
    if (!m_connected) {
        emit errorOccurred(QStringLiteral("订阅失败：尚未连接 broker"));
        return;
    }
    sendPacket(buildSubscribePacket(m_nextPacketId++, topic, qos));
}

void MqttWsClient::publish(const QString &topic, const QByteArray &payload)
{
    if (!m_connected) {
        emit errorOccurred(QStringLiteral("发布失败：尚未连接 broker"));
        return;
    }
    sendPacket(buildPublishPacket(topic, payload));
}

void MqttWsClient::onBinaryMessage(const QByteArray &data)
{
    // WebSocket 是消息流：MQTT 报文可能被拆帧，也可能多帧粘在一起，
    // 统一进缓冲区后按「剩余长度」逐包切分。
    m_rxBuffer.append(data);
    processIncoming();
}

void MqttWsClient::processIncoming()
{
    for (;;) {
        if (m_rxBuffer.size() < 2)
            return;                                   // 连固定头都不够

        int offset = 1;                               // 跳过报文类型字节
        int remaining = 0;
        const int rc = readRemainingLength(m_rxBuffer, offset, remaining);
        if (rc == -1)
            return;                                   // 长度字段还没收全
        if (rc == -2) {
            m_rxBuffer.clear();
            emit errorOccurred(QStringLiteral("MQTT 报文长度字段非法，已清空接收缓冲区"));
            return;
        }

        const int total = offset + remaining;
        if (m_rxBuffer.size() < total)
            return;                                   // 报文还没收全

        const QByteArray packet = m_rxBuffer.left(total);
        m_rxBuffer.remove(0, total);
        handlePacket(packet);
    }
}

void MqttWsClient::handlePacket(const QByteArray &packet)
{
    if (packet.size() < 2)
        return;

    const quint8 header = quint8(packet.at(0));
    const int type = header >> 4;

    switch (type) {
    case 2: {   // CONNACK
        const int returnCode = (packet.size() >= 4) ? int(quint8(packet.at(3))) : -1;
        if (returnCode == 0) {
            m_connected = true;
            m_pingTimer->start();
            emit connected();
        } else {
            emit errorOccurred(QStringLiteral("MQTT 连接被拒绝，CONNACK 返回码 %1").arg(returnCode));
        }
        break;
    }
    case 3: {   // PUBLISH（broker -> 客户端）
        int offset = 1;
        int remaining = 0;
        if (readRemainingLength(packet, offset, remaining) != 0)
            return;

        const int qos = (header >> 1) & 0x03;

        if (offset + 2 > packet.size())
            return;
        const int topicLength = (int(quint8(packet.at(offset))) << 8)
                              | int(quint8(packet.at(offset + 1)));
        offset += 2;
        if (offset + topicLength > packet.size())
            return;

        const QString topic = QString::fromUtf8(packet.constData() + offset, topicLength);
        offset += topicLength;

        quint16 packetId = 0;
        if (qos > 0) {                            // QoS1/2 带 2 字节报文标识符
            if (offset + 2 > packet.size())
                return;
            packetId = quint16((int(quint8(packet.at(offset))) << 8)
                               | int(quint8(packet.at(offset + 1))));
            offset += 2;
        }
        if (offset > packet.size())
            return;

        emit messageReceived(topic, packet.mid(offset));

        if (qos == 1)                             // QoS1 必须回 PUBACK
            sendPubAck(packetId);
        break;
    }
    case 9:     // SUBACK
    case 13:    // PINGRESP
        break;
    default:
        break;
    }
}

// ============================================================================
//  第 3 节：MainWindow 构造 / 界面 / QSS
// ============================================================================
MainWindow::MainWindow(QWidget *parent)
    : QMainWindow(parent)
{
    setWindowTitle(QStringLiteral("云端考勤管理 · 数据大屏"));
    resize(1360, 800);

    buildUi();                             // 内部会调用 buildCharts()
    setStyleSheet(styleSheet());

    // ---- MQTT ----
    m_mqtt = new MqttWsClient(this);
    connect(m_mqtt, &MqttWsClient::connected,       this, &MainWindow::onMqttConnected);
    connect(m_mqtt, &MqttWsClient::disconnected,    this, &MainWindow::onMqttDisconnected);
    connect(m_mqtt, &MqttWsClient::errorOccurred,   this, &MainWindow::onMqttError);
    connect(m_mqtt, &MqttWsClient::messageReceived, this, &MainWindow::onAttendanceMessage);

    // 每 5 秒检查一次：断了就重连，大屏不会一直空白
    m_reconnectTimer = new QTimer(this);
    m_reconnectTimer->setInterval(5000);
    connect(m_reconnectTimer, &QTimer::timeout, this, &MainWindow::connectBroker);
    m_reconnectTimer->start();

    statusBar()->showMessage(QStringLiteral("正在连接 MQTT 服务器…"));
    connectBroker();
}

MainWindow::~MainWindow() = default;

void MainWindow::connectBroker()
{
    if (m_mqtt->isConnected() || m_mqtt->isConnecting())
        return;

    // clientId 必须唯一，否则会把同 ID 的另一个客户端顶下线
    const QString clientId = QStringLiteral("qt-dashboard-%1")
                                 .arg(QUuid::createUuid().toString(QUuid::WithoutBraces).left(8));

    m_connLabel->setText(QStringLiteral("MQTT：连接中…"));
    m_connLabel->setStyleSheet(QStringLiteral("color:#f39c12;"));

    m_mqtt->open(QUrl(QStringLiteral("ws://8.152.219.98:9001/mqtt")), clientId);
}

// ---------------------------------------------------------------------------
//  界面搭建：顶部操作区 + 左表格 / 右图表
// ---------------------------------------------------------------------------
void MainWindow::buildUi()
{
    QWidget *central = new QWidget(this);
    setCentralWidget(central);

    QVBoxLayout *root = new QVBoxLayout(central);
    root->setContentsMargins(16, 16, 16, 12);
    root->setSpacing(14);

    // ---------------- 顶部操作区 ----------------
    QFrame *topBar = new QFrame(central);
    topBar->setObjectName(QStringLiteral("topBar"));

    QHBoxLayout *topLayout = new QHBoxLayout(topBar);
    topLayout->setContentsMargins(18, 10, 18, 10);
    topLayout->setSpacing(14);

    QLabel *title = new QLabel(QStringLiteral("云端考勤管理 · 数据大屏"), topBar);
    title->setObjectName(QStringLiteral("titleLabel"));

    m_connLabel = new QLabel(QStringLiteral("MQTT：未连接"), topBar);
    m_connLabel->setObjectName(QStringLiteral("connLabel"));

    m_totalLabel = new QLabel(QStringLiteral("今日打卡总人数：<b>0</b>"), topBar);
    m_totalLabel->setObjectName(QStringLiteral("totalLabel"));
    m_totalLabel->setTextFormat(Qt::RichText);

    QPushButton *btnEnroll = new QPushButton(QStringLiteral("录入新员工"), topBar);
    btnEnroll->setObjectName(QStringLiteral("primaryButton"));
    btnEnroll->setCursor(Qt::PointingHandCursor);
    connect(btnEnroll, &QPushButton::clicked, this, &MainWindow::onEnrollClicked);

    QPushButton *btnDelete = new QPushButton(QStringLiteral("删除员工"), topBar);
    btnDelete->setObjectName(QStringLiteral("dangerButton"));
    btnDelete->setCursor(Qt::PointingHandCursor);
    connect(btnDelete, &QPushButton::clicked, this, &MainWindow::onDeleteClicked);

    topLayout->addWidget(title);
    topLayout->addStretch(1);
    topLayout->addWidget(m_connLabel);
    topLayout->addWidget(m_totalLabel);
    topLayout->addWidget(btnEnroll);
    topLayout->addWidget(btnDelete);

    root->addWidget(topBar);

    // ---------------- 主体 ----------------
    QSplitter *splitter = new QSplitter(Qt::Horizontal, central);
    splitter->setHandleWidth(10);
    splitter->setChildrenCollapsible(false);

    // 左：实时打卡记录
    QFrame *leftPanel = new QFrame(splitter);
    leftPanel->setObjectName(QStringLiteral("panel"));

    QVBoxLayout *leftLayout = new QVBoxLayout(leftPanel);
    leftLayout->setContentsMargins(14, 12, 14, 14);
    leftLayout->setSpacing(10);

    QLabel *leftTitle = new QLabel(QStringLiteral("实时打卡记录"), leftPanel);
    leftTitle->setObjectName(QStringLiteral("panelTitle"));

    m_table = new QTableWidget(0, 2, leftPanel);
    m_table->setHorizontalHeaderLabels(QStringList() << QStringLiteral("姓名")
                                                     << QStringLiteral("打卡时间"));
    m_table->horizontalHeader()->setSectionResizeMode(0, QHeaderView::Stretch);
    m_table->horizontalHeader()->setSectionResizeMode(1, QHeaderView::Stretch);
    m_table->verticalHeader()->setVisible(false);
    m_table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    m_table->setSelectionBehavior(QAbstractItemView::SelectRows);
    m_table->setSelectionMode(QAbstractItemView::SingleSelection);
    m_table->setShowGrid(false);
    m_table->setAlternatingRowColors(true);
    m_table->setWordWrap(false);

    leftLayout->addWidget(leftTitle);
    leftLayout->addWidget(m_table, 1);

    // 右：上环形图 / 下折线图
    QWidget *rightWrap = new QWidget(splitter);
    QVBoxLayout *rightLayout = new QVBoxLayout(rightWrap);
    rightLayout->setContentsMargins(0, 0, 0, 0);
    rightLayout->setSpacing(14);

    QFrame *piePanel = new QFrame(rightWrap);
    piePanel->setObjectName(QStringLiteral("panel"));
    QVBoxLayout *pieLayout = new QVBoxLayout(piePanel);
    pieLayout->setContentsMargins(10, 10, 10, 10);

    QFrame *linePanel = new QFrame(rightWrap);
    linePanel->setObjectName(QStringLiteral("panel"));
    QVBoxLayout *lineLayout = new QVBoxLayout(linePanel);
    lineLayout->setContentsMargins(10, 10, 10, 10);

    buildCharts();                 // 创建 m_pieView / m_lineView
    pieLayout->addWidget(m_pieView);
    lineLayout->addWidget(m_lineView);

    rightLayout->addWidget(piePanel, 1);
    rightLayout->addWidget(linePanel, 1);

    splitter->addWidget(leftPanel);
    splitter->addWidget(rightWrap);
    splitter->setStretchFactor(0, 4);
    splitter->setStretchFactor(1, 6);
    splitter->setSizes(QList<int>() << 520 << 840);

    root->addWidget(splitter, 1);
}

void MainWindow::buildCharts()
{
    // ---------------- 环形图：正常 / 迟到 ----------------
    m_pie = new QPieSeries();
    m_sliceNormal = m_pie->append(QStringLiteral("正常"), 0.0);
    m_sliceLate   = m_pie->append(QStringLiteral("迟到"), 0.0);
    m_sliceEmpty  = m_pie->append(QStringLiteral("暂无数据"), 1.0);

    m_pie->setHoleSize(0.42);        // 中间挖空 => 环形
    m_pie->setPieSize(0.78);

    m_pieChart = new QChart();
    m_pieChart->setTheme(QChart::ChartThemeDark);
    m_pieChart->addSeries(m_pie);
    m_pieChart->setTitle(QStringLiteral("今日打卡分布（暂无数据）"));
    m_pieChart->setBackgroundVisible(false);
    m_pieChart->setPlotAreaBackgroundVisible(false);
    m_pieChart->setTitleBrush(QBrush(QColor("#e8f1ff")));
    m_pieChart->legend()->setAlignment(Qt::AlignBottom);
    m_pieChart->legend()->setLabelColor(QColor("#c9d6e8"));
    m_pieChart->setAnimationOptions(QChart::SeriesAnimations);

    // 颜色必须放在 setTheme 之后，否则会被主题覆盖
    m_sliceNormal->setColor(QColor("#2ecc71"));
    m_sliceLate->setColor(QColor("#e67e22"));
    m_sliceEmpty->setColor(QColor("#39424f"));
    m_sliceNormal->setBorderColor(QColor("#121a25"));
    m_sliceLate->setBorderColor(QColor("#121a25"));
    m_sliceEmpty->setBorderColor(QColor("#121a25"));
    m_sliceNormal->setLabelVisible(true);
    m_sliceLate->setLabelVisible(true);

    m_pieView = new QChartView(m_pieChart, this);
    m_pieView->setRenderHint(QPainter::Antialiasing);
    m_pieView->setMinimumHeight(240);
    m_pieView->setFrameShape(QFrame::NoFrame);

    // ---------------- 折线图：今日累计打卡 ----------------
    m_line = new QLineSeries();
    m_line->setName(QStringLiteral("累计打卡"));
    m_line->setPen(QPen(QColor("#3fa9f5"), 3));
    m_line->setPointsVisible(true);

    m_lineChart = new QChart();
    m_lineChart->setTheme(QChart::ChartThemeDark);
    m_lineChart->addSeries(m_line);
    m_lineChart->setTitle(QStringLiteral("今日打卡趋势"));
    m_lineChart->setBackgroundVisible(false);
    m_lineChart->setPlotAreaBackgroundVisible(false);
    m_lineChart->setTitleBrush(QBrush(QColor("#e8f1ff")));
    m_lineChart->legend()->setVisible(false);
    m_lineChart->setAnimationOptions(QChart::NoAnimation);

    m_axisTime = new QDateTimeAxis();
    m_axisTime->setFormat(QStringLiteral("HH:mm"));
    m_axisTime->setTickCount(6);
    m_axisTime->setLabelsColor(QColor("#8fa3bd"));
    m_axisTime->setGridLineColor(QColor("#24334a"));
    m_axisTime->setLinePenColor(QColor("#24334a"));

    m_axisValue = new QValueAxis();
    m_axisValue->setLabelFormat(QStringLiteral("%d"));
    m_axisValue->setLabelsColor(QColor("#8fa3bd"));
    m_axisValue->setGridLineColor(QColor("#24334a"));
    m_axisValue->setLinePenColor(QColor("#24334a"));
    m_axisValue->setRange(0, 5);

    m_lineChart->addAxis(m_axisTime, Qt::AlignBottom);
    m_lineChart->addAxis(m_axisValue, Qt::AlignLeft);
    m_line->attachAxis(m_axisTime);
    m_line->attachAxis(m_axisValue);

    // 先放一个 0 点并给时间范围，否则空图看不到坐标轴
    const QDateTime now = QDateTime::currentDateTime();
    m_firstPoint = now;
    m_lastPoint = now;
    m_line->append(now.toMSecsSinceEpoch(), 0);
    m_axisTime->setRange(now.addSecs(-300), now.addSecs(300));

    m_lineView = new QChartView(m_lineChart, this);
    m_lineView->setRenderHint(QPainter::Antialiasing);
    m_lineView->setMinimumHeight(200);
    m_lineView->setFrameShape(QFrame::NoFrame);
}

// ---------------------------------------------------------------------------
//  深色科技风 QSS
// ---------------------------------------------------------------------------
QString MainWindow::styleSheet() const
{
    return QStringLiteral(R"QSS(
/* ================= 全局 ================= */
QWidget {
    background-color: #0f141b;
    color: #d7e3f4;
    font-family: "Microsoft YaHei", "Noto Sans CJK SC", "WenQuanYi Micro Hei", sans-serif;
    font-size: 13px;
}

/* ================= 顶部标题与信息 ================= */
QLabel#titleLabel {
    font-size: 20px;
    font-weight: bold;
    color: #4fc3f7;
    letter-spacing: 2px;
}
QLabel#totalLabel { font-size: 14px; color: #e8f1ff; }
QLabel#connLabel  { font-size: 13px; color: #f39c12; }
QLabel#panelTitle { font-size: 15px; font-weight: bold; color: #9fd7ff; }

/* ================= 面板 ================= */
QFrame#topBar, QFrame#panel {
    background-color: #131a24;
    border: 1px solid #1f2a38;
    border-radius: 10px;
}
QFrame#panel { background-color: #121a25; }

/* ================= 按钮 ================= */
QPushButton {
    background-color: #1b2836;
    border: 1px solid #2c3f56;
    border-radius: 6px;
    padding: 7px 18px;
    color: #d7e3f4;
}
QPushButton:hover   { background-color: #24384e; border-color: #3fa9f5; }
QPushButton:pressed { background-color: #17222e; }

QPushButton#primaryButton {
    background-color: #1565c0;
    border: 1px solid #1e88e5;
    color: #ffffff;
    font-weight: bold;
}
QPushButton#primaryButton:hover { background-color: #1e88e5; }

QPushButton#dangerButton {
    background-color: #7b1f1f;
    border: 1px solid #c62828;
    color: #ffdede;
    font-weight: bold;
}
QPushButton#dangerButton:hover { background-color: #9c2828; }

/* ================= 表格 ================= */
QTableWidget {
    background-color: #0d131b;
    alternate-background-color: #111a24;
    border: 1px solid #1f2a38;
    border-radius: 8px;
    gridline-color: #1f2a38;
    selection-background-color: #1d3a57;
    selection-color: #ffffff;
}
QHeaderView::section {
    background-color: #16202c;
    color: #9fd7ff;
    padding: 8px;
    border: none;
    border-bottom: 1px solid #24334a;
    font-weight: bold;
}
QTableWidget::item { padding: 6px; }

/* ================= 滚动条 ================= */
QScrollBar:vertical {
    background: #0d131b;
    width: 10px;
    margin: 0;
    border-radius: 5px;
}
QScrollBar::handle:vertical {
    background: #2c3f56;
    border-radius: 5px;
    min-height: 24px;
}
QScrollBar::handle:vertical:hover { background: #3fa9f5; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background: transparent; }

/* ================= 对话框 / 输入框 / 状态栏 ================= */
QDialog { background-color: #121a25; }
QLineEdit {
    background-color: #0d131b;
    border: 1px solid #2c3f56;
    border-radius: 6px;
    padding: 7px 10px;
    color: #e8f1ff;
}
QLineEdit:focus { border-color: #3fa9f5; }
QStatusBar { background-color: #0d131b; color: #8fa3bd; }
QToolTip {
    background-color: #1b2836;
    color: #e8f1ff;
    border: 1px solid #2c3f56;
    padding: 4px;
}
)QSS");
}

// ============================================================================
//  第 4 节：业务槽函数
// ============================================================================
void MainWindow::onMqttConnected()
{
    m_connLabel->setText(QStringLiteral("MQTT：已连接"));
    m_connLabel->setStyleSheet(QStringLiteral("color:#2ecc71;"));

    // 订阅打卡主题（QoS1：broker 至少投递一次）
    m_mqtt->subscribe(QStringLiteral("attendance/checkin"), 1);
    statusBar()->showMessage(QStringLiteral("已连接 MQTT，正在监听 attendance/checkin"), 5000);
}

void MainWindow::onMqttDisconnected()
{
    m_connLabel->setText(QStringLiteral("MQTT：未连接"));
    m_connLabel->setStyleSheet(QStringLiteral("color:#f39c12;"));
    statusBar()->showMessage(QStringLiteral("MQTT 连接已断开，5 秒后自动重连"), 5000);
}

void MainWindow::onMqttError(const QString &message)
{
    // 不弹窗（断网时会刷屏），只写日志 + 状态栏
    qWarning() << "[MQTT]" << message;
    m_connLabel->setText(QStringLiteral("MQTT：异常"));
    m_connLabel->setStyleSheet(QStringLiteral("color:#e74c3c;"));
    statusBar()->showMessage(message, 6000);
}

// ---------------------------------------------------------------------------
//  收到打卡：{"name":"张三","time":"2026-11-14 08:30:00"}
// ---------------------------------------------------------------------------
void MainWindow::onAttendanceMessage(const QString &topic, const QByteArray &payload)
{
    if (topic != QLatin1String("attendance/checkin"))
        return;                                    // 只关心打卡主题

    QJsonParseError parseError;
    const QJsonDocument document = QJsonDocument::fromJson(payload, &parseError);
    if (parseError.error != QJsonParseError::NoError || !document.isObject()) {
        qWarning() << "[打卡] JSON 解析失败:" << parseError.errorString() << payload;
        return;
    }

    const QJsonObject object = document.object();
    const QString name = object.value(QStringLiteral("name")).toString().trimmed();
    if (name.isEmpty()) {
        qWarning() << "[打卡] 缺少 name 字段，已忽略:" << payload;
        return;
    }

    // 时间按终端约定解析，解析失败就用本机时间
    const QString timeText = object.value(QStringLiteral("time")).toString();
    QDateTime when = QDateTime::fromString(timeText, QStringLiteral("yyyy-MM-dd HH:mm:ss"));
    if (!when.isValid())
        when = QDateTime::fromString(timeText, Qt::ISODate);
    if (!when.isValid())
        when = QDateTime::currentDateTime();

    // 08:30 之后算迟到（题目假定）
    static const QTime kLateLine(8, 30, 0);
    const bool late = when.time() > kLateLine;

    addRecord(name, when, late);

    statusBar()->showMessage(QStringLiteral("收到打卡：%1  %2%3")
                                 .arg(name, timeText,
                                      late ? QStringLiteral("（迟到）") : QString()),
                             4000);
}

// ---------------------------------------------------------------------------
//  录入新员工：姓名 + 照片 -> 128 维特征 -> terminal/register
// ---------------------------------------------------------------------------
void MainWindow::onEnrollClicked()
{
    QDialog dialog(this);
    dialog.setWindowTitle(QStringLiteral("录入新员工"));
    dialog.setMinimumWidth(440);

    QVBoxLayout *layout = new QVBoxLayout(&dialog);
    layout->setContentsMargins(22, 20, 22, 16);
    layout->setSpacing(12);

    QLabel *hint = new QLabel(
        QStringLiteral("填写姓名并选择一张正面人脸照片（单人、清晰、不逆光）"), &dialog);
    hint->setWordWrap(true);
    layout->addWidget(hint);

    QLineEdit *nameEdit = new QLineEdit(&dialog);
    nameEdit->setPlaceholderText(QStringLiteral("例如：张三"));
    QFormLayout *form = new QFormLayout();
    form->addRow(QStringLiteral("姓名"), nameEdit);
    layout->addLayout(form);

    QLabel *preview = new QLabel(QStringLiteral("尚未选择照片"), &dialog);
    preview->setFixedSize(260, 260);
    preview->setAlignment(Qt::AlignCenter);
    preview->setStyleSheet(QStringLiteral(
        "border: 1px dashed #2c3f56; border-radius: 8px; color: #6b7d94;"));
    layout->addWidget(preview, 0, Qt::AlignHCenter);

    QHBoxLayout *fileRow = new QHBoxLayout();
    QPushButton *chooseButton = new QPushButton(QStringLiteral("选择照片…"), &dialog);
    QLabel *pathLabel = new QLabel(QStringLiteral("未选择"), &dialog);
    pathLabel->setObjectName(QStringLiteral("connLabel"));
    fileRow->addWidget(chooseButton);
    fileRow->addWidget(pathLabel, 1);
    layout->addLayout(fileRow);

    QDialogButtonBox *buttons =
        new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, &dialog);
    buttons->button(QDialogButtonBox::Ok)->setText(QStringLiteral("提取特征并下发"));
    buttons->button(QDialogButtonBox::Cancel)->setText(QStringLiteral("取消"));
    layout->addWidget(buttons);

    QString imagePath;    // 对话框存活期间有效，lambda 用引用捕获即可

    // —— 选择照片 + 预览 ——
    connect(chooseButton, &QPushButton::clicked, &dialog, [&]() {
        const QString path = QFileDialog::getOpenFileName(
            &dialog, QStringLiteral("选择人脸照片"), QString(),
            QStringLiteral("图片文件 (*.jpg *.jpeg *.png *.bmp)"));
        if (path.isEmpty())
            return;

        const QPixmap pixmap(path);
        if (pixmap.isNull()) {
            QMessageBox::warning(&dialog, QStringLiteral("提示"),
                                 QStringLiteral("图片无法读取，请换一张。"));
            return;
        }

        imagePath = path;
        pathLabel->setText(QFileInfo(path).fileName());
        preview->setPixmap(pixmap.scaled(preview->size() - QSize(8, 8),
                                         Qt::KeepAspectRatio, Qt::SmoothTransformation));
    });

    // —— 确定：先校验输入，再关闭对话框 ——
    connect(buttons, &QDialogButtonBox::accepted, &dialog, [&]() {
        if (nameEdit->text().trimmed().isEmpty()) {
            QMessageBox::warning(&dialog, QStringLiteral("提示"),
                                 QStringLiteral("请先输入姓名。"));
            return;
        }
        if (imagePath.isEmpty()) {
            QMessageBox::warning(&dialog, QStringLiteral("提示"),
                                 QStringLiteral("请先选择一张照片。"));
            return;
        }
        dialog.accept();
    });
    connect(buttons, &QDialogButtonBox::rejected, &dialog, &QDialog::reject);

    if (dialog.exec() != QDialog::Accepted)
        return;

    const QString name = nameEdit->text().trimmed();

    // ---- 提取特征（耗时，用等待光标提示）----
    QByteArray featureBytes;
    QString error;
    QApplication::setOverrideCursor(Qt::WaitCursor);
    const bool ok = extractFeature(imagePath, featureBytes, error);
    QApplication::restoreOverrideCursor();

    if (!ok) {
        QMessageBox::critical(this, QStringLiteral("录入失败"), error);
        return;
    }

    if (!m_mqtt->isConnected()) {
        QMessageBox::warning(this, QStringLiteral("未连接"),
                             QStringLiteral("MQTT 尚未连接，特征已提取但未能下发。\n"
                                            "请检查网络后重试。"));
        return;
    }

    // ---- 组装 JSON 并发布 ----
    QJsonObject object;
    object.insert(QStringLiteral("name"), name);
    object.insert(QStringLiteral("feature"), QString::fromLatin1(featureBytes.toBase64()));

    m_mqtt->publish(QStringLiteral("terminal/register"),
                    QJsonDocument(object).toJson(QJsonDocument::Compact));

    QMessageBox::information(
        this, QStringLiteral("已下发"),
        QStringLiteral("员工「%1」的人脸特征（%2 字节）已发送到 terminal/register。")
            .arg(name).arg(featureBytes.size()));
}

// ---------------------------------------------------------------------------
//  删除员工：terminal/delete {"name":"张三"}
// ---------------------------------------------------------------------------
void MainWindow::onDeleteClicked()
{
    bool ok = false;
    const QString name = QInputDialog::getText(
        this, QStringLiteral("删除员工"), QStringLiteral("请输入要删除的员工姓名："),
        QLineEdit::Normal, QString(), &ok).trimmed();

    if (!ok || name.isEmpty())
        return;

    if (!m_mqtt->isConnected()) {
        QMessageBox::warning(this, QStringLiteral("未连接"),
                             QStringLiteral("MQTT 尚未连接，无法下发删除指令。"));
        return;
    }

    QJsonObject object;
    object.insert(QStringLiteral("name"), name);
    m_mqtt->publish(QStringLiteral("terminal/delete"),
                    QJsonDocument(object).toJson(QJsonDocument::Compact));

    QMessageBox::information(this, QStringLiteral("已发送"),
                             QStringLiteral("删除指令已发送：%1").arg(name));
}

// ============================================================================
//  第 5 节：OpenCV 模型与特征提取
// ============================================================================
QString MainWindow::findModelFile(const QString &fileName) const
{
    const QString appDir = QCoreApplication::applicationDirPath();
    const QString workDir = QDir::currentPath();

    const QStringList candidates = QStringList()
        << appDir  + QStringLiteral("/models")    << appDir  + QStringLiteral("/modles")
        << appDir  + QStringLiteral("/../models") << appDir  + QStringLiteral("/../modles")
        << workDir + QStringLiteral("/models")    << workDir + QStringLiteral("/modles")
        << workDir + QStringLiteral("/../models") << workDir + QStringLiteral("/../modles");

    for (const QString &dir : candidates) {
        const QString path = QDir(dir).filePath(fileName);
        if (QFile::exists(path))
            return path;
    }
    return QString();
}

bool MainWindow::ensureModels(QString &error)
{
    if (!m_detector.empty() && !m_recognizer.empty())
        return true;                      // 只加载一次，之后复用

    const QString yunetPath = findModelFile(QStringLiteral("yunet.onnx"));
    const QString sfacePath = findModelFile(QStringLiteral("face_recognizer_fast.onnx"));

    if (yunetPath.isEmpty() || sfacePath.isEmpty()) {
        error = QStringLiteral(
            "找不到模型文件。\n"
            "请在程序所在目录下建 models/（或 modles/）目录，放入：\n"
            "    yunet.onnx\n"
            "    face_recognizer_fast.onnx\n"
            "（已查找：程序目录、当前工作目录及其 models/、modles/ 子目录）");
        return false;
    }

    try {
        // YuNet：score 0.9 / NMS 0.3 / topK 5000，CPU
        m_detector = cv::FaceDetectorYN::create(yunetPath.toStdString(), "",
                                                cv::Size(320, 320),
                                                0.9f, 0.3f, 5000, 0, 0);
        m_recognizer = cv::FaceRecognizerSF::create(sfacePath.toStdString(), "", 0, 0);
    } catch (const cv::Exception &e) {
        error = QStringLiteral("模型加载失败：%1").arg(QString::fromStdString(e.what()));
        m_detector.release();
        m_recognizer.release();
        return false;
    }

    if (m_detector.empty() || m_recognizer.empty()) {
        error = QStringLiteral("模型加载失败（返回空对象），请确认 onnx 文件完整。");
        return false;
    }
    return true;
}

bool MainWindow::extractFeature(const QString &imagePath, QByteArray &featureBytes, QString &error)
{
    if (!ensureModels(error))
        return false;

    // ---- 1) 读图：先 imread；失败再 QFile + imdecode（兼容中文路径）----
    cv::Mat image = cv::imread(imagePath.toStdString(), cv::IMREAD_COLOR);
    if (image.empty()) {
        QFile file(imagePath);
        if (file.open(QIODevice::ReadOnly)) {
            const QByteArray blob = file.readAll();
            const std::vector<uchar> buffer(blob.begin(), blob.end());
            image = cv::imdecode(buffer, cv::IMREAD_COLOR);
        }
    }
    if (image.empty()) {
        error = QStringLiteral("图片读取失败：%1").arg(imagePath);
        return false;
    }

    // ---- 2) 预处理：超大照片先等比缩小 ----
    // YuNet 的 anchor 是按中等分辨率训练的。手机照片动辄 3000x4000，
    // 直接喂进去不仅慢（几十秒），而且人脸相对网络过大，容易「一个都检不到」。
    // 长边压到 1024 足够做人脸检测与 112x112 对齐。
    const int kMaxSide = 1024;
    double    detectScale = 1.0;
    cv::Mat   detectImage = image;
    const int longSide = std::max(image.cols, image.rows);
    if (longSide > kMaxSide) {
        detectScale = double(kMaxSide) / double(longSide);
        cv::resize(image, detectImage, cv::Size(), detectScale, detectScale, cv::INTER_AREA);
    }

    // ---- 3) YuNet 检测 ----
    m_detector->setInputSize(detectImage.size());
    cv::Mat faces;
    m_detector->detect(detectImage, faces);

    // 0.9 阈值漏检时（侧脸 / 逆光 / 小脸），把阈值降到 0.6 再试一次
    if (faces.empty() || faces.rows < 1) {
        qWarning() << "[录入] 0.9 阈值未检出人脸，降阈值重试。原图" << image.cols << "x" << image.rows
                   << " 检测尺寸" << detectImage.cols << "x" << detectImage.rows;
        m_detector->setScoreThreshold(0.6f);
        m_detector->detect(detectImage, faces);
        m_detector->setScoreThreshold(0.9f);
    }

    if (faces.empty() || faces.rows < 1) {
        error = QStringLiteral("没有检测到人脸（图片 %1×%2，检测尺寸 %3×%4）。\n"
                               "请换一张正面、清晰、人脸占画面 1/5 以上、不逆光的照片。")
                    .arg(image.cols).arg(image.rows)
                    .arg(detectImage.cols).arg(detectImage.rows);
        return false;
    }

    // ---- 4) 取面积最大的一张脸（合影时取离镜头最近的）----
    int bestRow = 0;
    float bestArea = 0.0f;
    for (int r = 0; r < faces.rows; ++r) {
        const float *p = faces.ptr<float>(r);
        const float area = p[2] * p[3];
        if (area > bestArea) {
            bestArea = area;
            bestRow = r;
        }
    }

    // ---- 5) 关键点对齐 + 128 维特征 ----
    // 用缩放后的图做对齐即可：alignCrop 内部固定输出 112x112，与源图分辨率无关。
    cv::Mat aligned;
    m_recognizer->alignCrop(detectImage, faces.row(bestRow), aligned);
    if (aligned.empty()) {
        error = QStringLiteral("人脸对齐失败（人脸可能过于偏斜或贴边）。");
        return false;
    }

    cv::Mat feature;
    m_recognizer->feature(aligned, feature);
    if (feature.empty()) {
        error = QStringLiteral("特征提取失败。");
        return false;
    }

    // ---- 6) 【关键】整理成「连续、独占的 1x128 CV_32F」----
    //   OpenCV 的 Mat 可能是带步长的非连续内存，直接 memcpy 会取到垃圾；
    //   clone() 保证连续且独占，reshape() 保证形状为 1x128。
    if (feature.type() != CV_32F)
        feature.convertTo(feature, CV_32F);
    if (!feature.isContinuous())
        feature = feature.clone();

    feature = feature.reshape(1, 1);
    feature = feature.clone();

    if (feature.total() != 128) {
        error = QStringLiteral("特征维度异常：%1（应为 128）").arg(feature.total());
        return false;
    }

    // QByteArray 内部会拷贝一份，feature 出作用域也没关系（无内存泄漏）
    featureBytes = QByteArray(reinterpret_cast<const char *>(feature.data),
                              int(feature.total() * sizeof(float)));

    if (featureBytes.size() != 512) {
        error = QStringLiteral("特征长度异常：%1 字节（应为 512）").arg(featureBytes.size());
        return false;
    }
    return true;
}

// ============================================================================
//  第 6 节：表格与图表更新
// ============================================================================
void MainWindow::addRecord(const QString &name, const QDateTime &when, bool late)
{
    // 新记录插到第 0 行
    m_table->insertRow(0);

    QTableWidgetItem *nameItem = new QTableWidgetItem(name);
    QTableWidgetItem *timeItem =
        new QTableWidgetItem(when.toString(QStringLiteral("yyyy-MM-dd HH:mm:ss")));
    nameItem->setTextAlignment(Qt::AlignCenter);
    timeItem->setTextAlignment(Qt::AlignCenter);
    if (late)
        timeItem->setForeground(QBrush(QColor("#f39c12")));    // 迟到标橙色

    m_table->setItem(0, 0, nameItem);
    m_table->setItem(0, 1, timeItem);

    // 只保留最近 200 条，长时间运行不撑内存
    while (m_table->rowCount() > 200)
        m_table->removeRow(m_table->rowCount() - 1);

    m_table->scrollToTop();

    // 统计
    ++m_total;
    if (late)
        ++m_late;
    else
        ++m_normal;

    updateTotalLabel();
    updatePie();
    updateLine(when);
}

void MainWindow::updateTotalLabel()
{
    m_totalLabel->setText(QStringLiteral("今日打卡总人数：<b>%1</b>").arg(m_total));
}

void MainWindow::updatePie()
{
    if (m_total <= 0) {
        m_sliceNormal->setValue(0.0);
        m_sliceLate->setValue(0.0);
        m_sliceEmpty->setValue(1.0);
        m_pieChart->setTitle(QStringLiteral("今日打卡分布（暂无数据）"));
        return;
    }

    m_sliceEmpty->setValue(0.0);
    m_sliceNormal->setValue(double(m_normal));
    m_sliceLate->setValue(double(m_late));
    m_sliceNormal->setLabel(QStringLiteral("正常 %1").arg(m_normal));
    m_sliceLate->setLabel(QStringLiteral("迟到 %1").arg(m_late));
    m_pieChart->setTitle(QStringLiteral("今日打卡分布：正常 %1 · 迟到 %2")
                             .arg(m_normal).arg(m_late));
}

void MainWindow::updateLine(const QDateTime &when)
{
    if (m_firstPoint.isNull())
        m_firstPoint = when;
    m_lastPoint = when;

    m_line->append(when.toMSecsSinceEpoch(), m_total);

    // 只保留最近 600 个采样点
    if (m_line->count() > 600) {
        m_line->removePoints(0, m_line->count() - 600);
        m_firstPoint = QDateTime::fromMSecsSinceEpoch(qint64(m_line->at(0).x()));
    }

    QDateTime left = m_firstPoint;
    QDateTime right = m_lastPoint;
    if (left.secsTo(right) < 600) {        // 跨度太小会把点挤成竖线
        left = left.addSecs(-300);
        right = right.addSecs(300);
    }
    m_axisTime->setRange(left, right);
    m_axisValue->setRange(0, qMax(5, m_total + 1));
}
