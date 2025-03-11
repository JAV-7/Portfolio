#include "Score.h"
#include <QFont>

Score::Score(QGraphicsItem *parent): QGraphicsTextItem(parent)
{
    //Initialize score to 0
    score = 0;

    //Display text
    setPlainText(QString("Score: " + QString::number(score)));
    setDefaultTextColor(Qt::cyan);
    setFont(QFont("showcard gothic",18));
}

void Score::increase()
{
    score++;
    //Update score
    setPlainText(QString("Score: " + QString::number(score)));

}

int Score::getScore()
{
    return score;
}
