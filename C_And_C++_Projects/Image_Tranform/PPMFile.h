#ifndef PPMFILE_H_
#define PPMFILE_H_

//Non visible structure for ADT user
typedef struct strRGBColor{
    unsigned char red, green, blue;
} rgbColor; 

typedef struct strPPMFile* PPMFile;

PPMFile ppmFile_create(char*);
void ppmFile_destroy(PPMFile);
void ppmFile_blackAndWhite(PPMFile);
void ppmFile_grayscale(PPMFile);
void ppmFile_compress(PPMFile);
void ppmFile_dithering(PPMFile);

#endif