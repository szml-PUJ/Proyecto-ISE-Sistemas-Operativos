#define _POSIX_C_SOURCE 200809L

/*
Autores: Juan Camilo Maldonado, Samuel Zeudec , Daniel Plazas
Fichero: servidorISE.c
Objetivo del fichero: Implementacion del Control Proceso Principal ISE. Toda la logica real 
(hilos, semaforos , pipe nominal y calculo del ISE).

*/


#include <stdio.h> 
#include <stdlib.h> //Libreria para funciones como atoi, calloc , free y otras funciones relacionada con memoria
#include <string.h>
#include <unistd.h> //Liberia para funciones del sistema como sleep y read
#include <fcntl.h> //Librearia para trabajar con archivos como O_RDWR
#include <errno.h> //Libreria para saber si ocurrio algun error
#include <time.h>
#include <poll.h> //libreria que permite ver si hay datos disponibles para leer en un pipe
#include <signal.h>
#include <sys/stat.h> //Librearia para trabajar con archivos y pipes
#include <pthread.h> //Libreria para manejar y crear hilos
#include <semaphore.h>

#include "servidorISE.h"

//Constantes que usa el archivo para manejar el numero maximo de agentes y los sectores que se van a usar para calcular el ISE

#define MAX_AGENTES    4   //Maximo numero de agentes que se pueden conectar al servidor
#define NUM_SECTORES   5  //Sectores Industria, Servicios, Consumidores, Comercio, Construccion


static const double PESOS_SECTOR[NUM_SECTORES] = { 0.40, 0.30, 0.20, 0.05, 0.05 }; //peso de cada sector para el calculo del ISE
static const char* NOMBRE_SECTOR[NUM_SECTORES] = {"Industria", "Servicios", "Consumidores", "Comercio", "Construccion"};

//Informacion que se guarda para cada agente

typedef struct {
    MensajeEncuesta* buffer;
    int tam;
    int head;
    int tail;

    sem_t espacios_libres;
    sem_t items_disponibles;

    char pais[ISE_MAX_PAIS_LEN];
    int  ocupado;
    int  conexion_impresa;

    double suma_sector[NUM_SECTORES];
    long   cuenta_sector[NUM_SECTORES];
    long   total_encuestas;
} AgenteSlot;


//Estas variables se usan solamente dentro de este archivo

static AgenteSlot agentes[MAX_AGENTES];
static int hilos_max;
static int tam_buffer;
static int tiempo_limite;
static char nombre_pipe[256];

static pthread_mutex_t mutex_asignacion = PTHREAD_MUTEX_INITIALIZER; //Mutex se usa para evitar problemas al 
//asignar un slot a un pais nuevo (para que no se asignen dos slots al mismo pais si llegan dos mensajes de ese pais al mismo tiempo)
static volatile sig_atomic_t tiempo_terminado = 0; //Variable que indica si el tiempo limite del servidor se ha cumplido 
//(se usa para que los hilos consumidores puedan terminar si el tiempo se cumple antes de recibir un mensaje de FIN de su agente)

/*
  * Espera a que haya espacio o datos en el semaforo.
  * Se revisa cada segundo si ya termino el tiempo del servidor.
  */
static int esperar_con_limite(sem_t* sem) {
    while (!tiempo_terminado) {
		struct timespec ts; //Estructura que se usa para guardar el tiempo actual y agregarle 1 segundo para esperar a que el semaforo se libere
		clock_gettime(CLOCK_REALTIME, &ts); //Obtenemos el tiempo actual
		ts.tv_sec += 1; //Agregamos 1 segundo al tiempo actual para esperar 1 segundo
		int r = sem_timedwait(sem, &ts); //Esperamos a que el semaforo se libere, pero con un limite de tiempo de 1 segundo
		if (r == 0) return 0; //Si el semaforo se libero a tiempo, retornamos 0
		if (errno != ETIMEDOUT) return -1; //Si ocurrio un error distinto a timeout, retornamos -1
    }
    return -1;
}


/*
*Busca un slot libre para un pais nuevo, o devuelve el slot ya asignado a ese pais si ya existe.
*Si el pais no existe y no hay slots libres, busca un espacio libre para asignarlo 
*/

static int obtener_slot(const char* pais) {
	pthread_mutex_lock(&mutex_asignacion); //Bloqueamos el mutex para evitar que dos hilos accedan a la misma variable al mismo tiempo
	int libre = -1; //Variable que guarda el indice del primer slot libre que se encuentra
	for (int i = 0; i < hilos_max; i++) { // Recorremos todos los slots para buscar uno libre o uno que ya tenga el pais asignado
		if (agentes[i].ocupado && strcmp(agentes[i].pais, pais) == 0) { //Si el slot ya tiene el pais asignado, retornamos el indice del slot
			pthread_mutex_unlock(&mutex_asignacion); //Desbloqueamos el mutex antes de retornar el indice del slot
            return i;
        }
		if (!agentes[i].ocupado && libre == -1) libre = i;//Si el slot esta libre y no hemos encontrado un slot libre antes, guardamos el indice del slot libre
    }
	if (libre != -1) { //Si encontramos un slot libre, lo asignamos al pais y lo marcamos como ocupado
		strncpy(agentes[libre].pais, pais, ISE_MAX_PAIS_LEN - 1); //Se copia el nombre del pais asegurandose q no se pase tel tamanio del buffer
		agentes[libre].ocupado = 1; //Marcamos el slot como ocupado
    }
	pthread_mutex_unlock(&mutex_asignacion); //Desbloqueamos el mutex antes de retornar el indice del slot libre o -1 si no hay slots libres
	return libre;//Retornamos el indice del slot libre o -1 si no hay slots libres
}
//  Mutex se encarga de proteger la asignacion de slots a paises nuevos, para que no se asignen dos slots al mismo pais si llegan dos mensajes de ese pais al mismo tiempo


//Se crea un hilo consumidor por agente 
typedef struct { int indice; } ArgConsumidor;
/*
 * Este hilo toma los mensajes del buffer y los procesa.
 * Si recibe un dato, lo suma.
 * Si recibe MSG_FIN, muestra el reporte del Agente y termina.
 */
static void* hilo_consumidor(void* arg_void) {
	int idx = ((ArgConsumidor*)arg_void)->indice; //Se obtiene el indice del slot que se le asigno al hilo consumidor
	AgenteSlot* slot = &agentes[idx]; //Se obtiene el puntero al slot que se le asigno al hilo consumidor

	while (1) { //Bucle infinito que se ejecuta hasta que se recibe un mensaje de fin o se cumple el tiempo limite del servidor
		if (esperar_con_limite(&slot->items_disponibles) != 0) break; //Si el semaforo de items disponibles no se libera en el tiempo limite del servidor, se rompe el bucle y se termina el hilo consumidor

		MensajeEncuesta msg = slot->buffer[slot->tail]; //Se obtiene el mensaje del buffer circular del slot que se le asigno al hilo consumidor
		slot->tail = (slot->tail + 1) % slot->tam;//Se actualiza el indice del tail del buffer circular del slot que se le asigno al hilo consumidor
		sem_post(&slot->espacios_libres);//Se libera un espacio en el buffer circular del slot que se le asigno al hilo consumidor

		if (msg.tipo == MSG_FIN) { //Si se recibe un mensaje de fin, se imprime el reporte del Agente y se termina el hilo consumidor
			printf("\n[Reporte parcial - Agente %s]\n", slot->pais); //Se imprime el reporte parcial del Agente
			for (int s = 0; s < NUM_SECTORES; s++) { //Se recorre cada sector para calcular el promedio de sentimiento y el numero de encuestas recibidas
				double prom = slot->cuenta_sector[s] > 0 ? slot->suma_sector[s] / slot->cuenta_sector[s] : 0.0; //Se calcula el promedio de sentimiento del sector, si no hay encuestas recibidas, el promedio es 0
                printf("  %-13s: %6ld encuestas, sentimiento promedio %.2f%%\n", 
                    NOMBRE_SECTOR[s], slot->cuenta_sector[s], prom);
            }
			printf("  Total %s: %ld encuestas recibidas.\n", slot->pais, slot->total_encuestas);//Se imprime el total de encuestas recibidas por el Agente
            break;
        }
		else if (msg.tipo == MSG_DATO) { //Si se recibe un mensaje de dato, se suma el sentimiento del sector correspondiente y se incrementa el contador de encuestas recibidas por ese sector
			if (msg.id_sector >= 0 && msg.id_sector < NUM_SECTORES) { //Si el id del sector es valido, se suma el sentimiento del sector correspondiente y se incrementa el contador de encuestas recibidas por ese sector
				slot->suma_sector[msg.id_sector] += msg.sentimiento; 
				slot->cuenta_sector[msg.id_sector]++; 
				slot->total_encuestas++; 
            }
        }
    }
    return NULL; 
}

/*
 * Este hilo es el encargado de leer los mensajes del pipe.
 * Luego busca a que Agente pertenece cada mensaje y lo guarda
 * en el buffer correspondiente.
 */
static void* hilo_receptor(void* arg) {
	(void)arg; //No se usa el argumento, pero se pone para cumplir con la firma de la funcion pthread_create
	int fd = open(nombre_pipe, O_RDWR); /* O_RDWR evita EOF cuando no hay escritores */ 
    //Se abre el pipe nominal en modo lectura y escritura para evitar que se reciba EOF cuando no hay escritores   

	if (fd < 0) { //Si no se pudo abrir el pipe nominal, se imprime un mensaje de error y se termina el hilo receptor
        perror("Error al abrir el pipe nominal");
		tiempo_terminado = 1; //Se activa la bandera de tiempo terminado para que los hilos consumidores puedan terminar si el tiempo se cumple antes de recibir un mensaje de FIN de su agente
        return NULL;
    }

	while (!tiempo_terminado) { //Bucle infinito que se ejecuta hasta que se cumple el tiempo limite del servidor
		struct pollfd pfd = { .fd = fd, .events = POLLIN };//Se crea un struct pollfd para usar la funcion poll y esperar a que haya datos disponibles para leer en el pipe nominal
		int listo = poll(&pfd, 1, 500);//Se espera a que haya datos disponibles para leer en el pipe nominal, con un timeout de 500ms
		if (listo <= 0) continue; //Si no hay datos disponibles para leer en el pipe nominal, se continua con la siguiente iteracion del bucle

		MensajeEncuesta msg;//Se crea un struct MensajeEncuesta para guardar el mensaje recibido del pipe nominal
		ssize_t n = read(fd, &msg, sizeof(msg));//Se lee el mensaje del pipe nominal y se guarda en el struct MensajeEncuesta
		if (n != (ssize_t)sizeof(msg)) continue; //Si no se pudo leer el mensaje completo del pipe nominal, se continua con la siguiente iteracion del bucle

		int idx = obtener_slot(msg.pais); //Se obtiene el indice del slot correspondiente al pais del mensaje recibido, si el pais no tiene un slot asignado, se asigna uno nuevo
		if (idx == -1) { //Si no se pudo asignar un slot al pais del mensaje recibido, se imprime un mensaje de advertencia y se descarta el mensaje
            fprintf(stderr, "Aviso: se alcanzo el maximo de %d Agentes, se descarta mensaje de %s\n",hilos_max, msg.pais);
            continue;
        }
		AgenteSlot* slot = &agentes[idx]; //Se obtiene un puntero al slot correspondiente al pais del mensaje recibido
		if (msg.tipo == MSG_CONEXION && !slot->conexion_impresa) { //Si se recibe un mensaje de conexion y no se ha impreso el mensaje de conexion del Agente, se imprime el mensaje de conexion del Agente
            printf("Conexion establecida con Agente: %s\n", msg.pais);
			slot->conexion_impresa = 1; //Se marca el slot como que ya se imprimio el mensaje de conexion del Agente para no imprimirlo nuevamente
            continue;
        }

		if (esperar_con_limite(&slot->espacios_libres) != 0) continue; //Si el semaforo de espacios libres no se libera en el tiempo limite del servidor, se descarta el mensaje
		slot->buffer[slot->head] = msg; //se guarda el mensaje en el buffer circular del slot correspondiente al pais del mensaje recibido
		slot->head = (slot->head + 1) % slot->tam; //Se actualiza el indice del head del buffer circular del slot correspondiente al pais del mensaje recibido
		sem_post(&slot->items_disponibles); //Se libera un item disponible en el buffer circular del slot correspondiente al pais del mensaje recibido para que el hilo consumidor pueda procesarlo
    }
    close(fd);
    return NULL;
}

/*
 * Este hilo controla el tiempo que puede funcionar el servidor.
 * Cuando se acaba el tiempo, cambia la variable tiempo_terminado.
 */
static void* hilo_temporizador(void* arg) {
    (void)arg;
    sleep(tiempo_limite);
    tiempo_terminado = 1;
    return NULL;
}

/*
 * Prepara los espacios de los Agentes antes de crear los hilos.
 * Tambien reserva memoria y deja los semaforos listos para usarse.
 */
static void inicializar_slots(void) {
    for (int i = 0; i < hilos_max; i++) {
        agentes[i].buffer = calloc(tam_buffer, sizeof(MensajeEncuesta));
        agentes[i].tam = tam_buffer;
        agentes[i].head = agentes[i].tail = 0;
        agentes[i].ocupado = 0;
        agentes[i].conexion_impresa = 0;
        agentes[i].total_encuestas = 0;
        memset(agentes[i].suma_sector, 0, sizeof(agentes[i].suma_sector));
        memset(agentes[i].cuenta_sector, 0, sizeof(agentes[i].cuenta_sector));
        sem_init(&agentes[i].espacios_libres, 0, tam_buffer);
        sem_init(&agentes[i].items_disponibles, 0, 0);
    }
}


/* Falta funcion para imprimir el resumen, funciones publicas del menu como parsear argumentos, 
* validar los argumentos y  y la funcion para correr el servidor /

