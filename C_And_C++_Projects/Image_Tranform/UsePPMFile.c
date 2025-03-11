#include "PPMFile.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

int main(void){
    PPMFile pf1 = NULL, pf2 = NULL;
    char * FileName1 = (char*) malloc(256 * sizeof(char));
    char *FileName2  = (char*) malloc(256 * sizeof(char));;
    int o = -1;

    while(pf1 == NULL){
        scanf("%s",FileName1);
        pf1 = ppmFile_create(FileName1);
    }
    //printf("Archivo 1 leido exitosamente\n");
    while(pf2 == NULL){
        scanf("%s",FileName2);
        pf2 = ppmFile_create(FileName2);
    }
    //printf("Archivo 2 leido exitosamente\n");

    while(1){
        scanf("%d",&o);
        //exit
        if (o == 0)
        {
            //printf("Instancias destruidas, adios");
            free(FileName1);
            free(FileName2);
            ppmFile_destroy(pf1);
            ppmFile_destroy(pf2);
            break;
        }

        //Si fue una opcion valida, evalua cual proceso ejecutara
        if(o == 1){
            ppmFile_blackAndWhite(pf1);
            ppmFile_blackAndWhite(pf2);
            //printf("Black and white version successfully completed\n");
        }else if(o == 2){
            ppmFile_grayscale(pf1);
            ppmFile_grayscale(pf2);
            //printf("Grey scale version successfully completed\n");
        }else if(o == 3){
            ppmFile_compress(pf1);
            ppmFile_compress(pf2);
            //printf("Compress version successfully completed\n");
        }else if(o == 4){
            ppmFile_dithering(pf1);
            //ppmFile_dithering(pf2);
            //printf("Dithering version successfully completed\n");
        }
        //printf("Select an opcion again: \n");
    }
}