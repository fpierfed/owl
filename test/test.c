#include <gvc.h>
#include "grapher.h"



int grapher(char* data, char** result, unsigned int* resultSize) {
    GVC_t*          gvc;
    graph_t*        g;
    int             err = 0;


    if(data) {
        // Draw the graph.
        gvc = gvContext();

        g = agmemread(data);

        gvLayout(gvc, g, "dot");
        gvRenderData(gvc, g, "svg", result, resultSize);

        gvFreeLayout(gvc, g);
        agclose(g);
        err = gvFreeContext(gvc);
    }
    return(err);
}



