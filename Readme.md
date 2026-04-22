# Taller: Paradigma MapReduce en Python Puro

**Curso:** ABD 2026-1 | ULIMA  
**Sesión:** 3 — Paradigma MapReduce  
**Tecnología:** Python 3 (sin librerías externas y con mrjob)

## Objetivo

Comprender el modelo funcional de MapReduce implementando las funciones `map` y `reduce` desde cero en Python, trabajando con pares clave-valor.

## Prerequisitos

- Python 3 instalado
- Conocimientos básicos de funciones y listas en Python

## Conceptos clave

- **Map:** función que recibe un par `(clave, valor)` y emite uno o más pares `(clave_intermedia, valor_intermedio)`. Es **stateless** (sin estado compartido).
- **Reduce:** función que recibe una `(clave, [lista de valores])` y emite un resultado agregado.
- **Shuffle & Sort:** paso intermedio que agrupa todos los valores por clave antes del Reduce.

---

## Ejercicio 1 — Función Map básica

Dado el siguiente texto, implementa una función `map_words(line)` que reciba una línea de texto y retorne una lista de pares `(palabra, 1)` para cada palabra.

```python
lineas = [
    "big data es el futuro",
    "hadoop usa mapreduce para big data",
    "mapreduce es un paradigma funcional"
]
```

**Tu función debe:**
- Convertir cada palabra a minúsculas
- Ignorar espacios en blanco adicionales
- Retornar una lista de tuplas `(palabra, 1)`

**Ejemplo de salida esperada para la primera línea:**
```
[('big', 1), ('data', 1), ('es', 1), ('el', 1), ('futuro', 1)]
```

---

## Ejercicio 2 — Shuffle & Sort manual

Usando la salida del Ejercicio 1, implementa la función `shuffle_and_sort(pairs)` que:
- Recibe una lista de pares `(clave, valor)`
- Agrupa todos los valores bajo la misma clave
- Retorna un diccionario `{clave: [lista de valores]}`

**Ejemplo de salida esperada:**
```python
{
  'big': [1, 1],
  'data': [1, 1],
  'mapreduce': [1, 1],
  'es': [1, 1],
  ...
}
```

---

## Ejercicio 3 — Función Reduce

Implementa la función `reduce_sum(key, values)` que recibe una clave y su lista de valores, y retorna el par `(clave, suma_total)`.

Luego aplícala al resultado del Ejercicio 2 para obtener el conteo final de palabras.

**Salida esperada (parcial):**
```
('big', 2)
('data', 2)
('mapreduce', 2)
('es', 2)
...
```

---

## Ejercicio 4 — MapReduce completo: promedio por categoría

Dado el siguiente dataset de ventas:

```python
ventas = [
    ("Lima", 1500),
    ("Arequipa", 800),
    ("Lima", 2200),
    ("Cusco", 600),
    ("Arequipa", 1100),
    ("Lima", 900),
    ("Cusco", 750),
    ("Arequipa", 950),
]
```

Implementa un pipeline MapReduce completo que calcule el **promedio de ventas por ciudad**:

1. **Map:** emite `(ciudad, (monto, 1))` para cada venta
2. **Shuffle & Sort:** agrupa por ciudad
3. **Reduce:** calcula `(ciudad, promedio)` sumando montos y conteos

**Salida esperada:**
```
('Lima', 1533.33)
('Arequipa', 950.0)
('Cusco', 675.0)
```

---

## Ejercicio 5 — Máximo por categoría

Dado el siguiente dataset de temperaturas:

```python
temperaturas = [
    ("Lima", 28), ("Cusco", 15), ("Lima", 31),
    ("Arequipa", 22), ("Cusco", 12), ("Lima", 25),
    ("Arequipa", 19), ("Cusco", 18), ("Arequipa", 24),
]
```

Implementa un pipeline MapReduce que encuentre la **temperatura máxima por ciudad**.

- La función Map debe emitir `(ciudad, temperatura)`
- La función Reduce debe retornar `(ciudad, max_temperatura)`

---

## Ejercicio 6 — Reto: MapReduce funcional con `map()` y `reduce()` de Python

Reimplementa el conteo de palabras del Ejercicio 3, pero esta vez usando exclusivamente las funciones built-in de Python:
- `map()` para la fase Map
- `functools.reduce()` para la fase Reduce
- Ningún bucle `for` explícito

```python
from functools import reduce

texto = "big data hadoop mapreduce big data hadoop big"
# Tu solución aquí...
```

# Uso de librería mrjob

## Prerrequisitos

```bash
pip install mrjob
```

Cada ejercicio puede ejecutarse localmente con:
```bash
python nombre_script.py archivo_entrada.txt
```

## Tópico 1: Filtrado de Datos

> Uso del Mapper para limpiar registros (eliminar nulos, valores inválidos o incompletos).

### Ejercicio 7 — Filtrar registros con campos nulos en un CSV de empleados

**Descripción:** Dado un archivo CSV con columnas `id,nombre,departamento,salario`, eliminar las filas que tengan algún campo vacío o nulo y contar cuántos registros válidos quedan por departamento.

**Archivo de entrada (`empleados.csv`):**
```
1,Ana,Ventas,3500
2,,Marketing,2800
3,Luis,Ventas,
4,María,TI,4200
5,Pedro,Marketing,3100
6,,,3000
7,Sofía,TI,3900
```

### Ejercicio 8 — Filtrar transacciones con montos negativos o cero

**Descripción:** Un log de transacciones contiene líneas con formato `transaction_id,cliente,monto`. Filtrar registros con montos ≤ 0 o no numéricos, y calcular el total de transacciones válidas por cliente.

**Archivo de entrada (`transacciones.txt`):**
```
T001,cliente_A,150.5
T002,cliente_B,-30.0
T003,cliente_A,0
T004,cliente_C,200.0
T005,cliente_B,abc
T006,cliente_C,75.0
T007,cliente_A,320.0
```

### Ejercicio 9 — Limpiar log de accesos web con IPs y códigos de estado inválidos

**Descripción:** Un log web tiene el formato `ip,ruta,codigo_http`. Conservar solo registros con IPs válidas (formato básico `x.x.x.x`) y códigos HTTP entre 100 y 599. Contar accesos válidos por ruta.

**Archivo de entrada (`access.log`):**
```
192.168.1.1,/home,200
999.999.0.1,/login,200
10.0.0.5,/home,404
abc.def.ghi,/api,500
172.16.0.3,/home,200
10.0.0.5,/api,99
192.168.1.1,/login,301
```

**Ejecución:**
```bash
python filtro_logs.py access.log
```

**Salida esperada:**
```
"/api"      1
"/home"     3
"/login"    2
```

## Tópico 2: Agregaciones Complejas

> Calcular estadísticas como promedios, máximos y mínimos sobre datasets de ventas.

### Ejercicio 10 — Promedio de ventas por región

**Descripción:** Un archivo de ventas tiene el formato `region,vendedor,monto_venta`. Calcular el promedio de ventas por región.

**Archivo de entrada (`ventas.csv`):**
```
Norte,Ana,1200
Sur,Luis,800
Norte,Pedro,1500
Sur,María,950
Norte,Ana,1100
Sur,Luis,700
Este,Carlos,2000
Este,Carlos,1800
```

**Ejecución:**
```bash
python promedio_ventas.py ventas.csv
```

**Salida esperada:**
```
"Este"     1900.0
"Norte"    1266.67
"Sur"      816.67
```

### Ejercicio 11 — Venta máxima y mínima por categoría de producto

**Descripción:** Dataset con formato `categoria,producto,precio_unitario,cantidad`. Calcular el ingreso total por transacción (`precio × cantidad`) y reportar el ingreso máximo y mínimo por categoría.

**Archivo de entrada (`productos.csv`):**
```
Electrónica,Laptop,2500,2
Ropa,Camisa,80,10
Electrónica,Celular,1200,3
Ropa,Pantalón,120,5
Electrónica,Audífonos,350,8
Ropa,Camisa,80,3
Alimentos,Arroz,5,100
Alimentos,Aceite,12,50
```

**Ejecución:**
```bash
python max_min_ventas.py productos.csv
```

**Salida esperada:**
```
"Alimentos"      {"maximo": 600.0, "minimo": 500.0, "transacciones": 2}
"Electrónica"    {"maximo": 5000.0, "minimo": 2800.0, "transacciones": 3}
"Ropa"           {"maximo": 800.0, "minimo": 240.0, "transacciones": 3}
```

### Ejercicio 12 — Top vendedor por mes (mayor monto acumulado)

**Descripción:** Registros con formato `año_mes,vendedor,monto`. Identificar el vendedor con mayor venta acumulada en cada mes.

**Archivo de entrada (`ventas_mensuales.csv`):**
```
2026-01,Ana,3000
2026-01,Luis,4500
2026-01,Ana,2000
2026-02,Luis,1500
2026-02,Ana,5000
2026-02,Pedro,3200
2026-01,Pedro,1000
```

**Ejecución:**
```bash
python top_vendedor.py ventas_mensuales.csv
```

**Salida esperada:**
```
"2026-01"    {"top_vendedor": "Luis", "total": 4500.0}
"2026-02"    {"top_vendedor": "Ana", "total": 5000.0}
```

## Tópico 3: Encadenamiento de Pasos (Multi-step Jobs)

> La salida de un Reducer sirve como entrada del siguiente Mapper, permitiendo pipelines de procesamiento.

### Ejercicio 13 — Encontrar la palabra más frecuente en un corpus

**Descripción:** Pipeline de dos pasos: el primero cuenta frecuencias de palabras; el segundo selecciona la palabra con mayor frecuencia global.

**Archivo de entrada (`corpus.txt`):**
```
el gato come el pescado
el perro come la carne
el gato juega con el perro
la gata come pescado con el gato
```

**Ejecución:**
```bash
python palabra_mas_frecuente.py corpus.txt
```

**Salida esperada:**
```
"palabra_mas_frecuente"    {"palabra": "el", "frecuencia": 6}
```

### Ejercicio 14 — Pipeline: Normalizar ventas y luego calcular ranking

**Descripción:** Paso 1 convierte montos de distintas monedas a USD. Paso 2 ordena y asigna ranking global por monto convertido.

**Archivo de entrada (`ventas_monedas.csv`):**
```
vendedor_A,1000,PEN
vendedor_B,500,USD
vendedor_C,800,EUR
vendedor_A,200,USD
vendedor_B,1500,PEN
```

**Tasas de conversión:** PEN→USD: ×0.27 | EUR→USD: ×1.08

**Ejecución:**
```bash
python ranking_ventas.py ventas_monedas.csv
```

**Salida esperada:**
```
1    {"vendedor": "vendedor_C", "total_usd": 864.0}
2    {"vendedor": "vendedor_A", "total_usd": 470.0}
3    {"vendedor": "vendedor_B", "total_usd": 905.0}
```

### Ejercicio 15 — Detección de usuarios activos: sesiones → días activos → promedio

**Descripción:** Pipeline de 3 pasos. Paso 1: contar sesiones por usuario por día. Paso 2: contar días activos por usuario. Paso 3: calcular el promedio global de días activos.

**Archivo de entrada (`sesiones.txt`):**
```
usuario_1,2026-04-01
usuario_2,2026-04-01
usuario_1,2026-04-01
usuario_1,2026-04-02
usuario_3,2026-04-01
usuario_2,2026-04-02
usuario_2,2026-04-02
usuario_3,2026-04-03
```

**Ejecución:**
```bash
python usuarios_activos.py sesiones.txt
```

**Salida esperada:**
```
"usuario_1"    2
"usuario_2"    2
"usuario_3"    2
```

## Tópico 4: Joins en MapReduce

> Combinar dos fuentes de datos usando llaves comunes (Map-side o Reduce-side join).

### Ejercicio 16 — Join de empleados y departamentos

**Descripción:** Dos archivos: `empleados.txt` (id_emp, nombre, id_depto) y `departamentos.txt` (id_depto, nombre_depto). Producir un reporte con nombre del empleado y nombre de su departamento.

**Archivos de entrada:**

`empleados.txt`:
```
E01,Ana,D1
E02,Luis,D2
E03,María,D1
E04,Pedro,D3
```

`departamentos.txt`:
```
D1,Ventas
D2,Marketing
D3,TI
```

**Ejecución:**
```bash
python join_empleados.py empleados.txt departamentos.txt
```

**Salida esperada:**
```
"D1"    {"empleado": "Ana", "departamento": "Ventas"}
"D1"    {"empleado": "María", "departamento": "Ventas"}
"D2"    {"empleado": "Luis", "departamento": "Marketing"}
"D3"    {"empleado": "Pedro", "departamento": "TI"}
```

### Ejercicio 17 — Join de pedidos y clientes para calcular gasto total

**Descripción:** Dos archivos: `pedidos.txt` (id_pedido, id_cliente, monto) y `clientes.txt` (id_cliente, nombre, ciudad). Calcular el gasto total por cliente incluyendo su nombre y ciudad.

**Archivos de entrada:**

`pedidos.txt`:
```
P001,C01,250.0
P002,C02,180.0
P003,C01,320.0
P004,C03,90.0
P005,C02,430.0
```

`clientes.txt`:
```
C01,Sofía,Lima
C02,Carlos,Arequipa
C03,Elena,Cusco
```

**Ejecución:**
```bash
python join_pedidos_clientes.py pedidos.txt clientes.txt
```

**Salida esperada:**
```
"C01"    {"nombre": "Sofía", "ciudad": "Lima", "gasto_total": 570.0}
"C02"    {"nombre": "Carlos", "ciudad": "Arequipa", "gasto_total": 610.0}
"C03"    {"nombre": "Elena", "ciudad": "Cusco", "gasto_total": 90.0}
```

### Ejercicio 18 — Join de tres fuentes: productos, categorías y descuentos

**Descripción:** Tres archivos: `productos2.txt` (id_prod, nombre, id_cat), `categorias.txt` (id_cat, nombre_cat), `descuentos.txt` (id_prod, pct_descuento). Generar un reporte que muestre producto, categoría y su descuento aplicado.

**Archivos de entrada:**

`productos2.txt`:
```
PR01,Laptop,CAT_ELEC
PR02,Camisa,CAT_ROPA
PR03,Celular,CAT_ELEC
PR04,Zapatos,CAT_ROPA
```

`categorias.txt`:
```
CAT_ELEC,Electrónica
CAT_ROPA,Ropa y Calzado
```

`descuentos.txt`:
```
PR01,15
PR03,10
PR04,20
```

> **Nota importante:** Para el ejercicio 18 con 3 fuentes, la forma más práctica con mrjob es ejecutar los dos pasos con todos los archivos en el mismo comando:

**Ejecución:**
```bash
python join_tres_fuentes.py productos2.txt categorias.txt descuentos.txt
```

**Salida esperada:**
```
"PR01"    {"producto": "Laptop", "categoria": "Electrónica", "descuento_pct": 15}
"PR02"    {"producto": "Camisa", "categoria": "Ropa y Calzado", "descuento_pct": 0}
"PR03"    {"producto": "Celular", "categoria": "Electrónica", "descuento_pct": 10}
"PR04"    {"producto": "Zapatos", "categoria": "Ropa y Calzado", "descuento_pct": 20}
```


