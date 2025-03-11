#include "Health.h"
#include <QFont>
#include <QApplication>
#include <QLabel>
#include <QVBoxLayout>
#include <QWidget>
#include <QLabel>
#include <QTimer>

Health::Health(QGraphicsItem *parent): QGraphicsTextItem(parent)
{
    //Initialize health to 3
    health = 3;

    //Display text
    setPlainText(QString("Health: " + QString::number(health)));
    setDefaultTextColor(Qt::white);
    setFont(QFont("showcard gothic",18));
}

void Health::decrease()
{
    if(health > 0){
        health--;
        //Update health
        setPlainText(QString("Health: " + QString::number(health)));
    }
    else if(health == 0){
        health = -1;
        QWidget *gameOverWindow = new QWidget();
        gameOverWindow->setWindowTitle("Game Over");

        // Create a label to display the game over message
        QLabel *messageLabel = new QLabel("You lost! Game over");
        messageLabel->setAlignment(Qt::AlignCenter);
        messageLabel->setStyleSheet("font-size: 24px; color: white; font-family: 'Showcard Gothic';");

        // Create a layout and add the label
        QVBoxLayout *layout = new QVBoxLayout();
        layout->addWidget(messageLabel);

        gameOverWindow->setLayout(layout);
        gameOverWindow->setFixedSize(500, 150);
        gameOverWindow->setStyleSheet("background-image: url(:/images/background_image.png); background-repeat: no-repeat; background-position: center;");
        gameOverWindow->show();


        QTimer::singleShot(4000, QApplication::instance(), &QApplication::quit);//After 4 seconds, the app quits
    }

}

int Health::getHealth()
{
    return health;
}
