QT       += core gui
QT       += multimedia

greaterThan(QT_MAJOR_VERSION, 4): QT += widgets

CONFIG += c++17

# You can make your code fail to compile if it uses deprecated APIs.
# In order to do so, uncomment the following line.
#DEFINES += QT_DISABLE_DEPRECATED_BEFORE=0x060000    # disables all the APIs deprecated before Qt 6.0.0

SOURCES += \
    Bullet.cpp \
    Enemy.cpp \
    Game.cpp \
    GameOver.cpp \
    Health.cpp \
    MyRect.cpp \
    Score.cpp \
    main.cpp

HEADERS += \
    Bullet.h \
    Enemy.h \
    Game.h \
    GameOver.h \
    Health.h \
    MyRect.h \
    Score.h

FORMS +=

TRANSLATIONS += \
    airstrike_en_US.ts
CONFIG += lrelease
CONFIG += embed_translations

# Default rules for deployment.
qnx: target.path = /tmp/$${TARGET}/bin
else: unix:!android: target.path = /opt/$${TARGET}/bin
!isEmpty(target.path): INSTALLS += target

RESOURCES += \
    resource.qrc
