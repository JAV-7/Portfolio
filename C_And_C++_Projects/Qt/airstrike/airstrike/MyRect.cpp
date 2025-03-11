#include "MyRect.h"
#include <QGraphicsScene>
#include <QKeyEvent>
#include "Bullet.h"
#include "Enemy.h"

using namespace Qt; //Avoid typing Qt::Key_Name

MyRect::MyRect(QGraphicsItem * parent):QObject(), QGraphicsPixmapItem(parent)
{
    bulletSound = new QMediaPlayer(this);
    bulletOutput = new QAudioOutput(this);

    bulletSound->setAudioOutput(bulletOutput);
    bulletOutput->setMuted(false);
    bulletOutput->setVolume(25);
    bulletSound->setSource(QUrl("qrc:/music/gun_effect.mp3"));

    setPixmap(QPixmap(":/images/player_ship.png"));
}

void MyRect::keyPressEvent(QKeyEvent *event)
{
    //If left key is pressed
    if(event->key() == Key_Left || event->key() == Key_A){
        if(x() > 0)
        setPos(x()-10, y());//Move the position -10 x and keep y
    }

    //If right key is pressed
    else if(event->key() == Key_Right || event->key() == Key_D){
        if(x() + QPixmap(":/images/player_ship.png").width() < scene()->width())
        setPos(x()+10, y());//Move the position +10 x and keep y
    }

    //Create a bullet when clicking space
    else if(event->key() == Key_Space){
        Bullet * bullet = new Bullet();
        bullet->setPos(x()+32,y());//Set to rectangles x and y
        scene()->addItem(bullet);//Add to scene

        //play bullet sound
        if(bulletSound->playbackState() == QMediaPlayer::PlayingState){
            bulletSound->setPosition(0);
        }
        else if(bulletSound->playbackState() == QMediaPlayer::StoppedState){
            bulletSound->play();
        }
    }
}

void MyRect::spawn(){
    //Create an enemy
    Enemy * enemy = new Enemy();
    scene()->addItem(enemy);


}
