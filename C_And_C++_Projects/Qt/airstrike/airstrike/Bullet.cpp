#include "Bullet.h"
#include "Enemy.h"
#include "Game.h"
#include <QList>
#include <QGraphicsScene>

extern Game * game; //External global object

Bullet::Bullet(QGraphicsItem * parent):QObject(), QGraphicsPixmapItem(parent){
    //Draw graphics
    setPixmap(QPixmap(":/images/bullet_image.png"));

    // Connect
    timer = new QTimer();
    connect(timer,SIGNAL(timeout()),this,SLOT(move()));//Connect a signal with a slot. Connect timeout to move

    timer->start(50); //Every 50 ms, timout occurs

}

void Bullet::move(){
    //If bullet hits enemy, delete both
    QList<QGraphicsItem *> colliding_items = collidingItems();
    for(int i = 0; i < colliding_items.size(); ++i){
        if(typeid(*(colliding_items[i])) == typeid(Enemy)){//If type of object, dereference
            //Increase score
            game->score->increase();
            //Remove both
            scene()->removeItem(colliding_items[i]);
            scene()->removeItem(this);
            //Remove from heap
            delete colliding_items[i];
            delete this;
            return;
        }
    }

    setPos(x(), y()-10); //Move bullet up
    if(pos().y() < 50){//If bullet is out of scene, it's deleted
        scene()->removeItem(this);
        delete this;
    }

}
