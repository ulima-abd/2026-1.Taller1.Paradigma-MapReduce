# Taller: Paradigma MapReduce en Python Puro

**Curso:** ABD 2026-1 | ULIMA  
**Sesión:** 3 — Paradigma MapReduce  
**Tecnología:** Python 3 (sin librerías externas)  
**Duración estimada:** 60 minutos  

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
