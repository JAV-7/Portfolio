#include "Enemy.h"
#include <QTimer>
#include <stdlib.h>
#include <QGraphicsScene>
#include "Game.h"

extern Game * game;

Enemy::Enemy(QGraphicsItem * parent):QObject(), QGraphicsPixmapItem(parent){
    //Set random position
    int random_number = rand() % 700;
    setPos(random_number,0);

    setPixmap(QPixmap(":/images/enemy_ship.png"));

    QTimer * timer = new QTimer();
    connect(timer,SIGNAL(timeout()),this,SLOT(move()));

    timer->start(50);

}

void Enemy::move(){
    //Move enemy down
    setPos(x(), y()+5);
    if(pos().y() > scene()->height()){
        //decrease Health
        game->health->decrease();

        scene()->removeItem(this);
        delete this;
    }
}
