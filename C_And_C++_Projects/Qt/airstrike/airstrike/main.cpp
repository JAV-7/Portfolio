/* Airstriker game
 *
 * This project was used using QT creator
 * More about QT:
 */

#include <QApplication>
#include "Game.h"

Game * game; //game is global, so it is easy to access

int main(int argc, char *argv[])
{
    QApplication a(argc, argv);

    game = new Game();
    game->show();

    return a.exec();

}


