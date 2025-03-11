 #include "Game.h"
#include <QTimer>
#include <QGraphicsTextItem>
#include <QFont>
#include "Enemy.h"
#include <QMediaPlayer>
#include <QAudioOutput>
#include <QBrush>
#include <QImage>

using namespace Qt;

Game::Game(QWidget * parent) {
    parent = parent;//Nonsense line to stop producing "Unused parameter" error
    //Creating a scene
    scene = new QGraphicsScene();
    scene->setSceneRect(0,0,800,600);//Set size of scene
    //Background image
    setBackgroundBrush(QBrush(QImage(":/images/background_image.png")));//Apply image to background
    setWindowTitle("Space arcade");

    //
    setScene(scene);
    setHorizontalScrollBarPolicy(ScrollBarAlwaysOff);
    setVerticalScrollBarPolicy(ScrollBarAlwaysOff);
    setFixedSize(800,600);//Set size of view


    //Create a player
    player = new MyRect(); //By default, 0x0 pixels
    player->setPos(width()/2 - QPixmap(":/images/player_ship.png").width()/2, height() - QPixmap(":/images/player_ship.png").height());
    //Make item focusable so it responds to key typing
    player->setFlag(QGraphicsItem::ItemIsFocusable);
    player->setFocus();
    scene->addItem(player);

    //Create score
    score = new Score();
    scene->addItem(score);
    //Create health
    health = new Health();
    health->setPos(health->x(),health->y()+25);
    scene->addItem(health);
    //Spawn enemies
    QTimer * timer = new QTimer();
    QObject::connect(timer,SIGNAL(timeout()),player,SLOT(spawn()));
    timer->start(2000);


    //Play background music
    QMediaPlayer * music = new QMediaPlayer();
    QAudioOutput * audioOutput = new QAudioOutput();


    music->setAudioOutput(audioOutput);
    audioOutput->setMuted(false);
    audioOutput->setVolume(25);
    music->setSource(QUrl("qrc:/music/background_music.mp3"));

    music->play();

    if(music->playbackState() == QMediaPlayer::StoppedState){
        music->setPosition(0);
        music->play();
    }

    show();
}


