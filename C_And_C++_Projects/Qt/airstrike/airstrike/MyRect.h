#ifndef MYRECT_H
#define MYRECT_H

#include <QGraphicsPixmapItem>
#include <QGraphicsItem>
#include <QObject>
#include <QMediaPlayer>
#include <QAudioOutput>

class MyRect:public QObject, public QGraphicsPixmapItem{
    Q_OBJECT
public:
    MyRect(QGraphicsItem * parent=0);
    void keyPressEvent(QKeyEvent * event); //Allow response of buttons

public slots:
    void spawn();
private:
    QMediaPlayer * bulletSound;
    QAudioOutput * bulletOutput;
};

#endif // MYRECT_H
