// ============================================================================
//  main.cpp —— 程序入口
//
//  只做三件事：开启高 DPI 支持、创建 QApplication、显示 MainWindow。
//  其余逻辑全部在 MainWindow / MqttWsClient 里。
// ============================================================================

#include "mainwindow.h"

#include <QApplication>
#include <QCoreApplication>
#include <QtGlobal>

int main(int argc, char *argv[])
{
    // Qt 5.14 的高 DPI 支持需要在 QApplication 构造之前设置
    QCoreApplication::setAttribute(Qt::AA_EnableHighDpiScaling);
    QCoreApplication::setAttribute(Qt::AA_UseHighDpiPixmaps);

    QApplication app(argc, argv);
    app.setApplicationName(QStringLiteral("CloudDashboard"));
    app.setApplicationDisplayName(QStringLiteral("云端考勤管理与数据大屏"));

    MainWindow window;
    window.show();

    return app.exec();
}
