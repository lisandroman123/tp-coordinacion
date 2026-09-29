# Relacion entre SUM, Rol del Aggreggator y Join

Para resolver el problema de sincronizacion entre SUMs ya que el mensaje eof le llega simplemente a uno de ellos y la información de los clientes esta esparcida entre estos se colocó un exchange entre ellos. Esto implica que se lance un hilo SUM que está consumiendo de una cola propia y puede recibir 2 tipos de mensajes. El primer mensaje significa que tiene que comunicarle a otro SUM todos los datos que tenga de determinado cliente. Este mismo mensaje puede ser interpretado de 2 formas, si me llega [client_id,sum_id] y ese sum_id != ID, entonces me piden que envié información, mientras que si me llega [client_id, sum_id] y sum_id == ID, signfica que uno de los sums terminó de enviar información. No me interesa quién, simplemente me interesa saber que todos hayan terminado, con lo cual realizo una suma de todos los EOF recibidos y debe coincidir con la SUM_AMOUNT de esa instancia.
El segundo tipo de mensaje, son las frutas y cantidades de cliente pedido a otros sums. 
Esto implica que la informacion que manipula el SUM puede caer en una race condition entre el hilo principal y el hilo alterno que recibe informacion de los otros sums. 
Para solucionar este problema, se encapsuló en un Monitor el diccionario de clientes con frutas permitiendo que los hilos lo modifiquen de manera sincronizada. 
A su vez, tambien tengo que determinar cuando los otros sums terminaron de enviar su información para ello tengo que realizar un recuento. Una vez terminado el recuento, se determina el aggregator al que irá determinado cliente con una funcion muy simple de hash, ya que lo pide la consigna, no realizar broadcast.

En caso de que llegue el mensaje enviar información de tal cliente, puede que la otra instancia de sum este procesando un mensaje de ese cliente, con lo cual, se guarda el cliente con el que se está interactuando y solamente se permite acceder a la información una vez que se hayan terminado de hacer arreglos, para no enviar informacion corrupta. 

Puede que haya mas información de ese cliente además de la que proceso?
No, se supone que cada si la hay, está en otro sum, ya que los mensajes se consumen de a 1 y se los procesa, si habia más mensajes, fue consumido por otra instancia y eventualmente esa instancia se ocupará de la sincronización de los datos.

Con esta implementación garantizamos que todos los sums cuando reciben la información de sus pares, realicen una suma total de esa fruta de ese cliente. De esta manera cuando lo envio al aggregator, este no tiene nada para sumar, porque ya recibio la suma total de esa fruta.

# Ejemplo
En la siguiente imagen podemos observar como llegado a la instancia principal del sum, se recibe un eof del cliente 1 y este se encarga de iniciar el proceso entre sums para solicitar toda la informacion requerida sobre ese cliente con el mensaje:  1, sum_0
De esta manera, los sums saben de que cliente se quiere la información y a quien hay que enviarsela.
Cómo explique más arriba, el cliente 1,sum_1 si entrara por la cola que esta absorviendo sum_1, este interpretaría que uno de los sums terminó y puede realizar la suma de que uno más finalizó transmitiendo la información con ese cliente.
<div>
    <img src="./imgs/ejemplo_entre_instancias.png" alt="diagrama de ejemplo">
</div>



En esta solución, los aggregators, simplemente que reciben la informacion de cada cliente y la almacenan de forma ordenada. Una vez llegado el eof de ese cliente (puede que pasar que me llegue el eof de otro cliente del mismo sum?), se procede a enviar el TOP al join

Este último simplemente realiza un pasamanos, es decir deposita en la cola, el top recibido. Según lo charlado en clase, era una solución válida. 

