// ============================================================================
//  mainwindow.h —— 云端考勤管理与数据大屏
//
//  本文件包含两个类：
//    1. MqttWsClient  : 基于 QWebSocket 的极简 MQTT 3.1.1 客户端
//                       （QWebSocket 只是 WebSocket 传输层，“发到某个主题”
//                        必须自己拼 MQTT 报文，否则 broker 收到的是垃圾）
//    2. MainWindow    : 主窗口（表格 + 环形图 + 折线图 + 录入/删除）
//
//  依赖：Qt5 Widgets / Network / Charts / WebSockets + OpenCV(objdetect, dnn)
//  注意：所有 Qt 接口均为 Qt 5.14.2 可用版本，未使用 Qt6 专属 API。
// ============================================================================

#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QAbstractSocket>
#include <QByteArray>
#include <QDateTime>
#include <QMainWindow>
#include <QObject>
#include <QString>
#include <QUrl>

#include <opencv2/opencv.hpp>   // cv::FaceDetectorYN / cv::FaceRecognizerSF / cv::Mat

class QLabel;
class QPushButton;
class QTableWidget;
class QTimer;
class QWebSocket;

// Qt Charts 的类都在 QtCharts 命名空间里（Qt5），这里只做前置声明，
// 避免头文件把整个 QtCharts 都拉进来。
namespace QtCharts {
class QChart;
class QChartView;
class QPieSeries;
class QPieSlice;
class QLineSeries;
class QDateTimeAxis;
class QValueAxis;
}

// ============================================================================
//  MqttWsClient —— 最小可用 MQTT 3.1.1 over WebSocket 客户端
//
//  只实现本项目需要的报文：
//    发送：CONNECT / SUBSCRIBE / PUBLISH(QoS0) / PINGREQ / DISCONNECT
//    接收：CONNACK / SUBACK / PUBLISH / PINGRESP
//
//  为什么不用 QMqttClient：Qt 的 Mqtt 模块是独立的 QtMqtt，Qt 5.14.2 标准安装
//  里通常没有；题目要求的就是 QWebSocket 传输。
// ============================================================================
class MqttWsClient : public QObject
{
    Q_OBJECT

public:
    explicit MqttWsClient(QObject *parent = nullptr);
    ~MqttWsClient() override;

    // 连接 broker；url 形如 ws://8.152.219.98:9001/mqtt
    void open(const QUrl &url, const QString &clientId);
    void close();

    bool isConnected() const { return m_connected; }
    bool isConnecting() const;

    void subscribe(const QString &topic, int qos = 1);
    void publish(const QString &topic, const QByteArray &payload);

signals:
    void connected();                                             // CONNACK 成功
    void disconnected();                                          // 连接断开
    void errorOccurred(const QString &message);                    // 任何错误
    void messageReceived(const QString &topic, const QByteArray &payload); // 收到 PUBLISH

private slots:
    void onSocketConnected();
    void onSocketDisconnected();
    void onBinaryMessage(const QByteArray &data);
    void onSocketError(QAbstractSocket::SocketError error);
    void onPingTimer();

private:
    void sendPacket(const QByteArray &packet);
    void sendPubAck(quint16 packetId);
    void processIncoming();
    void handlePacket(const QByteArray &packet);
    QByteArray buildConnectPacket(const QString &clientId) const;
    QByteArray buildSubscribePacket(quint16 packetId, const QString &topic, int qos) const;
    QByteArray buildPublishPacket(const QString &topic, const QByteArray &payload) const;

    QWebSocket *m_ws = nullptr;
    QTimer     *m_pingTimer = nullptr;
    QByteArray  m_rxBuffer;          // 粘包 / 半包缓存
    QString     m_clientId;
    quint16     m_nextPacketId = 1;
    bool        m_connected = false;
};

// ============================================================================
//  MainWindow —— 数据大屏主窗口
// ============================================================================
class MainWindow : public QMainWindow
{
    Q_OBJECT

public:
    explicit MainWindow(QWidget *parent = nullptr);
    ~MainWindow() override;

private slots:
    void onEnrollClicked();       // 录入新员工
    void onDeleteClicked();       // 删除员工
    void onMqttConnected();
    void onMqttDisconnected();
    void onMqttError(const QString &message);
    void onAttendanceMessage(const QString &topic, const QByteArray &payload);
    void connectBroker();         // 连接 / 断线重连

private:
    // ---- 界面 ----
    void buildUi();
    void buildCharts();
    QString styleSheet() const;

    // ---- 模型与特征 ----
    QString findModelFile(const QString &fileName) const;
    bool ensureModels(QString &error);
    bool extractFeature(const QString &imagePath, QByteArray &featureBytes, QString &error);

    // ---- 数据与图表 ----
    void addRecord(const QString &name, const QDateTime &when, bool late);
    void updatePie();
    void updateLine(const QDateTime &when);
    void updateTotalLabel();

    // 顶部
    QLabel *m_totalLabel = nullptr;   // 今日打卡总人数
    QLabel *m_connLabel = nullptr;    // MQTT 连接状态
    // 左侧
    QTableWidget *m_table = nullptr;  // 姓名 / 打卡时间
    // 右侧
    QtCharts::QChartView   *m_pieView = nullptr;
    QtCharts::QChartView   *m_lineView = nullptr;
    QtCharts::QChart        *m_pieChart = nullptr;
    QtCharts::QChart        *m_lineChart = nullptr;
    QtCharts::QPieSeries    *m_pie = nullptr;
    QtCharts::QPieSlice     *m_sliceNormal = nullptr;
    QtCharts::QPieSlice     *m_sliceLate = nullptr;
    QtCharts::QPieSlice     *m_sliceEmpty = nullptr;   // 无数据时的占位扇区
    QtCharts::QLineSeries   *m_line = nullptr;
    QtCharts::QDateTimeAxis *m_axisTime = nullptr;
    QtCharts::QValueAxis    *m_axisValue = nullptr;

    // 统计
    int       m_total = 0;           // 今日打卡总人数
    int       m_normal = 0;          // 正常
    int       m_late = 0;            // 迟到
    QDateTime m_firstPoint;          // 折线图左端
    QDateTime m_lastPoint;           // 折线图右端

    // 通信
    MqttWsClient *m_mqtt = nullptr;
    QTimer       *m_reconnectTimer = nullptr;

    // OpenCV 模型（cv::Ptr = 智能指针，自动释放，不会泄漏）
    cv::Ptr<cv::FaceDetectorYN>   m_detector;
    cv::Ptr<cv::FaceRecognizerSF> m_recognizer;
};

#endif // MAINWINDOW_H
