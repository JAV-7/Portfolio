#ifndef BULLET_H
#define BULLET_H

#include <QGraphicsPixmapItem>
#include <QGraphicsItem>
#include <QObject> //Mandatory for signals and object
#include <QTimer>

class Bullet: public QObject, public QGraphicsPixmapItem{
    Q_OBJECT //Macro to allow bullet to handle signals
public:
    Bullet(QGraphicsItem * parent = 0);

public slots:
    void move();

private:
    QTimer * timer;
};


#endif // BULLET_H
