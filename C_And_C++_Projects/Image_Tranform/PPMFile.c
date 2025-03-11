#include "PPMFile.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

//Private functions
rgbColor** createImage(int width, int height);
void freeImage(rgbColor ** image, int height);
void saveImage(char * name, int width, int height, int max, rgbColor ** image);


struct strPPMFile{
    char* name;
    int nameLength;
    int width;
    int height;
    int max;
    rgbColor** image;
};

//Create ADT PPMFile
PPMFile ppmFile_create(char* name)
{
    //Open file
    FILE * f = fopen(name, "r");
    if (f == NULL)
    {
        printf("File does not exist or could not be read.\n");
        return NULL;
    }

    //Verify supported file
    char header[3];
    fscanf(f, "%2s", header);
    if (header[0] != 'P' || header[1] != '3')
    {
        printf("Unsupported file format.\n");
        fclose(f);
        return NULL;
    }

    //Create ADT
    PPMFile ppm = (PPMFile) malloc(sizeof(struct strPPMFile));
    if(ppm == NULL)
    {
        printf("Memory could not be allocated.\n");
        fclose(f);
        return NULL;
    }
    //Set name and nameLength
    ppm->name = (char*) malloc(strlen(name) + 1);
    if(ppm->name == NULL)
    {
        printf("Memory could not be allocated.\n");
        free(ppm);
        fclose(f);
        return NULL;
    }
    strcpy(ppm->name, name);
    ppm->nameLength = strlen(name);

    //Set width, height, and max
    fscanf(f, "%d %d", &ppm->width, &ppm->height);
    fscanf(f, "%d", &ppm->max);
    //Set image
    ppm->image = createImage(ppm->width, ppm->height);
    if (ppm->image == NULL)
    {
        printf("Error creating image.\n");
        freeImage(ppm->image, ppm->height);
        free(ppm);
        fclose(f);
        return NULL;
    }else{  
    for(int i = 0; i < ppm->height; ++i)
    {
        for(int j = 0; j < ppm->width; ++j)
        {
            fscanf(f, "%d %d %d", &ppm->image[i][j].red, &ppm->image[i][j].green, &ppm->image[i][j].blue);
        }
    }
    fclose(f); 
    }
    return ppm;
}

//Convert ppmFile to black and white
void ppmFile_blackAndWhite(PPMFile ppm)
{
    PPMFile bw_ppm = ppm;
    char * newName = (char*) malloc((bw_ppm->nameLength + strlen("bw_") +1) * sizeof(char));
    if (newName == NULL) {
        printf("Memory could not be allocated.\n");
        return;
    }
    strcpy(newName,"bw_");
    strcat(newName,bw_ppm->name);
    bw_ppm->name = newName;
    bw_ppm->max = 1; //Max value for black and white
    for(int i = 0; i < bw_ppm->height; ++i){
        for(int j = 0; j < bw_ppm->width; ++j){
            int bw =  (int) round(((bw_ppm->image[i][j].red+bw_ppm->image[i][j].green+bw_ppm->image[i][j].blue)/3.0)/255);
            bw_ppm->image[i][j].red = bw;
            bw_ppm->image[i][j].green = bw;
            bw_ppm->image[i][j].blue = bw;
        }
    }
    //printf("Black and white version is ready!\n");
    saveImage(bw_ppm->name, bw_ppm->width, bw_ppm->height, bw_ppm->max, bw_ppm->image);
    ppmFile_destroy(bw_ppm);
}

//Convert ppmFile to grayscale
void ppmFile_grayscale(PPMFile ppm)
{   
    PPMFile gs_ppm = ppm;
    char * newName = (char*) malloc((gs_ppm->nameLength + strlen("gs_")+1) * sizeof(char));
    if (newName == NULL) {
        printf("Memory could not be allocated.\n");
        return;
    }
    strcpy(newName,"gs_");
    strcat(newName,gs_ppm->name);
    gs_ppm->name = newName;
    for(int i = 0; i < gs_ppm->height; ++i){
        for(int j = 0; j < gs_ppm->width; ++j){
            int gs = (int) round((gs_ppm->image[i][j].red+gs_ppm->image[i][j].green+gs_ppm->image[i][j].blue)/3.0);
            gs_ppm->image[i][j].red = gs;
            gs_ppm->image[i][j].green = gs;
            gs_ppm->image[i][j].blue = gs;
        }
    }
    //printf("Gray scale version is ready!\n");
    saveImage(gs_ppm->name, gs_ppm->width, gs_ppm->height, gs_ppm->max, gs_ppm->image);
    ppmFile_destroy(gs_ppm);
}

//Convert ppmFile to compress
void ppmFile_compress(PPMFile ppm)
{
    PPMFile cp_ppm = ppm;
    char * newName = (char*) malloc((cp_ppm->nameLength + strlen("cmp_")+1) * sizeof(char));
    if (newName == NULL) {
        printf("Memory could not be allocated.\n");
        return;
    }
    strcpy(newName,"cmp_");
    strcat(newName,cp_ppm->name);
    cp_ppm->name = newName;
    cp_ppm->max = 1; //Max value for compressed
    for(int i = 0; i < cp_ppm->height; ++i){
        for(int j = 0; j < cp_ppm->width; ++j){
            int rc = (int) round(cp_ppm->image[i][j].red/255.0);
            int gc = (int) round(cp_ppm->image[i][j].green/255.0);
            int bc = (int) round(cp_ppm->image[i][j].blue/255.0);
            cp_ppm->image[i][j].red = rc;
            cp_ppm->image[i][j].green = gc;
            cp_ppm->image[i][j].blue = bc;
        }
    }
    //printf("Compressed version is ready!\n");
    saveImage(cp_ppm->name, cp_ppm->width, cp_ppm->height, cp_ppm->max, cp_ppm->image);
    ppmFile_destroy(cp_ppm);
}

//Convert ppmFile to dithering
void ppmFile_dithering(PPMFile ppm)
{
    PPMFile dh_ppm = ppm;
    char * newName = (char*) malloc((dh_ppm->nameLength + strlen("dth_")+1) * sizeof(char));
    if (newName == NULL) {
        printf("Memory could not be allocated.\n");
        return;
    }
    strcpy(newName,"dth_");
    strcat(newName,dh_ppm->name);
    dh_ppm->name = newName;
    dh_ppm->max = 1; //Max value for dithering
    //Calculate size
    if (dh_ppm->height % 2 != 0) dh_ppm->height -= 1;
    if (dh_ppm->width % 2 != 0) dh_ppm->width -= 1;

    if (dh_ppm->height <= 0 || dh_ppm->width <= 0)
    {
        printf("Error, size is not enoght to apply the dithering.\n");
        return;
    }    
    for(int i = 0; i < dh_ppm->height; i+=2){
        for(int j = 0; j < dh_ppm->width; j+=2){
            //printf("Procesando grupo [%d][%d]",i,j);
            float ar = (dh_ppm->image[i][j].red + dh_ppm->image[i+1][j].red + dh_ppm->image[i][j+1].red+ dh_ppm->image[i+1][j+1].red)/1020.0;
            float ag = (dh_ppm->image[i][j].green + dh_ppm->image[i+1][j].green + dh_ppm->image[i][j+1].green + dh_ppm->image[i+1][j+1].green)/1020.0;
            float ab = (dh_ppm->image[i][j].blue + dh_ppm->image[i+1][j].blue + dh_ppm->image[i][j+1].blue + dh_ppm->image[i+1][j+1].blue)/1020.0;
            //Red color average
            if(ar > 0.875){
                //Mask of a 100%
                dh_ppm->image[i][j].red = 1; //Upper left limit
                dh_ppm->image[i+1][j].red = 1; //Lower left limit
                dh_ppm->image[i][j+1].red = 1; //Upper right limit
                dh_ppm->image[i+1][j+1].red = 1; //Lower right limit
            }else if(ar > 0.625){
                //Mask of a 75%
                dh_ppm->image[i][j].red = 1; //Upper left limit
                dh_ppm->image[i+1][j].red = 1; //Lower left limit
                dh_ppm->image[i][j+1].red = 0; //Upper right limit
                dh_ppm->image[i+1][j+1].red = 1; //Lower right limit
            }else if(ar > 0.375){
                //Mask of a 50%
                dh_ppm->image[i][j].red = 1; //Upper left limit
                dh_ppm->image[i+1][j].red = 0; //Lower left limit
                dh_ppm->image[i][j+1].red = 0; //Upper right limit
                dh_ppm->image[i+1][j+1].red = 1; //Lower right limit
            }else if(ar == 0.125){
                //Mask of a 25%
                dh_ppm->image[i][j].red = 0; //Upper left limit
                dh_ppm->image[i+1][j].red = 0; //Lower left limit
                dh_ppm->image[i][j+1].red = 1; //Upper right limit
                dh_ppm->image[i+1][j+1].red = 0; //Lower right limit
            }else{
                //Mask of a 0%
                dh_ppm->image[i][j].red = 0; //Upper left limit
                dh_ppm->image[i+1][j].red = 0; //Lower left limit
                dh_ppm->image[i][j+1].red = 0; //Upper right limit
                dh_ppm->image[i+1][j+1].red = 0; //Lower right limit
            }

            //Green color average
            if(ag > 0.875){
                //Mask of a 100%
                dh_ppm->image[i][j].green = 1; //Upper left limit
                dh_ppm->image[i+1][j].green = 1; //Lower left limit
                dh_ppm->image[i][j+1].green = 1; //Upper right limit
                dh_ppm->image[i+1][j+1].green = 1; //Lower right limit
            }else if(ag > 0.625){
                //Mask of a 75%
                dh_ppm->image[i][j].green = 1; //Upper left limit
                dh_ppm->image[i+1][j].green = 1; //Lower left limit
                dh_ppm->image[i][j+1].green = 0; //Upper right limit
                dh_ppm->image[i+1][j+1].green = 1; //Lower right limit
            }else if(ag > 0.375){
                //Mask of a 50%
                dh_ppm->image[i][j].green = 1; //Upper left limit
                dh_ppm->image[i+1][j].green = 0; //Lower left limit
                dh_ppm->image[i][j+1].green = 0; //Upper right limit
                dh_ppm->image[i+1][j+1].green = 1; //Lower right limit
            }else if(ag == 0.125){
                //Mask of a 25%
                dh_ppm->image[i][j].green = 0; //Upper left limit
                dh_ppm->image[i+1][j].green = 0; //Lower left limit
                dh_ppm->image[i][j+1].green = 1; //Upper right limit
                dh_ppm->image[i+1][j+1].green = 0; //Lower right limit
            }else{
                //Mask of a 0%
                dh_ppm->image[i][j].green = 0; //Upper left limit
                dh_ppm->image[i+1][j].green = 0; //Lower left limit
                dh_ppm->image[i][j+1].green = 0; //Upper right limit
                dh_ppm->image[i+1][j+1].green = 0; //Lower right limit
            }

            //Blue color average
            if(ab > 0.875){
                //Mask of a 100%
                dh_ppm->image[i][j].blue = 1; //Upper left limit
                dh_ppm->image[i+1][j].blue = 1; //Lower left limit
                dh_ppm->image[i][j+1].blue = 1; //Upper right limit
                dh_ppm->image[i+1][j+1].blue = 1; //Lower right limit
            }else if(ab > 0.625){
                //Mask of a 75%
                dh_ppm->image[i][j].blue = 1; //Upper left limit
                dh_ppm->image[i+1][j].blue = 1; //Lower left limit
                dh_ppm->image[i][j+1].blue = 0; //Upper right limit
                dh_ppm->image[i+1][j+1].blue = 1; //Lower right limit
            }else if(ab > 0.375){
                //Mask of a 50%
                dh_ppm->image[i][j].blue = 1; //Upper left limit
                dh_ppm->image[i+1][j].blue = 0; //Lower left limit
                dh_ppm->image[i][j+1].blue = 0; //Upper right limit
                dh_ppm->image[i+1][j+1].blue = 1; //Lower right limit
            }else if(ab == 0.125){
                //Mask of a 25%
                dh_ppm->image[i][j].blue = 0; //Upper left limit
                dh_ppm->image[i+1][j].blue = 0; //Lower left limit
                dh_ppm->image[i][j+1].blue = 1; //Upper right limit
                dh_ppm->image[i+1][j+1].blue = 0; //Lower right limit
            }else{
                //Mask of a 0%
                dh_ppm->image[i][j].blue = 0; //Upper left limit
                dh_ppm->image[i+1][j].blue = 0; //Lower left limit
                dh_ppm->image[i][j+1].blue = 0; //Upper right limit
                dh_ppm->image[i+1][j+1].blue = 0; //Lower right limit
            }
        }
    }
    //printf("Dithering version is ready!\n");
    saveImage(dh_ppm->name, dh_ppm->width, dh_ppm->height, dh_ppm->max, dh_ppm->image);
    ppmFile_destroy(dh_ppm);
}

//Destro ADT
void ppmFile_destroy(PPMFile ppm)
{
    freeImage(ppm->image, ppm->height);
    free(ppm->name);
    free(ppm);
}

//Save image to device
void saveImage(char * name, int width, int height, int max, rgbColor ** image)
{
    FILE * f = NULL;
    f = fopen(name,"w");
    //Set width, height, and max
    fputs("P3\n", f);
    fprintf(f, "%d %d\n", width, height);
    fprintf(f, "%d\n", max);
    for(int i = 0; i < height; ++i)
    {
        for(int j = 0; j < width; ++j)
        {
            fprintf(f, "%d %d %d\n", image[i][j].red, image[i][j].green, image[i][j].blue);
        }
    }
    fclose(f);
}

//Free function
void freeImage(rgbColor ** image, int height)
{
    for(int i = 0; i < height; ++i)
        free(image[i]);
    free(image);
}

//Create image
rgbColor ** createImage(int width, int height)
{
    //printf("Width: %d \nHeight: %d\n",width, height);
    rgbColor ** image = (rgbColor**) malloc(height * sizeof(int*));
    if(image == NULL)
    {
        printf("Memory could not be allocated.\n");
        return NULL;
    }
    for(int i = 0; i < height; ++i)
    {
        image[i] = (rgbColor*) malloc(width * sizeof(rgbColor));
        if(image[i] == NULL)
        {
            printf("Memory could not be allocated.\n");
            freeImage(image, i+1);
            return NULL;
        }
    }
    return image;
}