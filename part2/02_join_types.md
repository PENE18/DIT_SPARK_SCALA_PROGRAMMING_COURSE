# 02 — Types de Jointures Spark Scala

## Syntaxe

```scala
val result = df1.join(df2, conditionJointure, "type_jointure")

// Même nom de colonne (équijointure raccourcie)
val result = df1.join(df2, "id")
val result = df1.join(df2, Seq("id", "dept_id"))    // Clés multiples

// Noms différents
val result = df1.join(df2, df1("emp_id") === df2("employee_id"))
```

---

## Tous les types de jointures

### INNER JOIN (défaut)

Retourne uniquement les lignes qui ont une correspondance dans les **deux** DataFrames.

```scala
val result = df1.join(df2, "id")             // inner par défaut
val result = df1.join(df2, "id", "inner")
```

```
df1:  [1, Alice]   [2, Bob]   [3, Carol]
df2:  [1, HR]      [2, IT]    [4, Legal]
Résultat: [1, Alice, HR]   [2, Bob, IT]    ← 3 et 4 exclus
```

### LEFT JOIN (left_outer)

Toutes les lignes de df1 + données correspondantes de df2. Non-correspondances → NULL.

```scala
val result = df1.join(df2, "id", "left")
val result = df1.join(df2, "id", "left_outer")   // identique
```

```
Résultat: [1, Alice, HR]   [2, Bob, IT]   [3, Carol, null]
```

### RIGHT JOIN (right_outer)

Toutes les lignes de df2 + données correspondantes de df1.

```scala
val result = df1.join(df2, "id", "right")
```

### FULL OUTER JOIN

Toutes les lignes des **deux** DataFrames, NULL là où il n'y a pas de correspondance.

```scala
val result = df1.join(df2, "id", "full")
val result = df1.join(df2, "id", "outer")
val result = df1.join(df2, "id", "full_outer")
```

### LEFT SEMI JOIN

Lignes de df1 qui **ont** une correspondance dans df2. Les colonnes de df2 ne sont **pas** incluses.

```scala
val result = df1.join(df2, "id", "leftsemi")     // Scala : leftsemi (pas semi)
val result = df1.join(df2, "id", "left_semi")
```

```
Résultat: [1, Alice]   [2, Bob]     ← Seulement colonnes df1, lignes correspondantes
```

### LEFT ANTI JOIN

Lignes de df1 qui **n'ont pas** de correspondance dans df2.

```scala
val result = df1.join(df2, "id", "leftanti")     // Scala : leftanti (pas anti)
val result = df1.join(df2, "id", "left_anti")
```

```
Résultat: [3, Carol]     ← Lignes dans df1 PAS dans df2
```

### CROSS JOIN (Produit cartésien)

Chaque combinaison de lignes. [ATTENTION] Taille = N × M — à utiliser avec extrême précaution.

```scala
val result = df1.crossJoin(df2)
val result = df1.join(df2, how = "cross")
```

---

## Tableau récapitulatif

| Type | Mots-clés Scala | Lignes retournées |
|------|-----------------|-------------------|
| Inner | `"inner"` | Correspondance dans les DEUX |
| Left Outer | `"left"`, `"left_outer"` | TOUTES depuis la gauche + correspondances droite |
| Right Outer | `"right"`, `"right_outer"` | TOUTES depuis la droite + correspondances gauche |
| Full Outer | `"full"`, `"outer"`, `"full_outer"` | TOUTES des deux côtés |
| Left Semi | **`"leftsemi"`**, `"left_semi"` | Lignes gauche qui correspondent (colonnes gauche uniquement) |
| Left Anti | **`"leftanti"`**, `"left_anti"` | Lignes gauche qui NE correspondent PAS |
| Cross | `"cross"` | Toutes les combinaisons (N × M) |

> [ATTENTION] **Différence Scala/Python :** En Python on écrit `"semi"` et `"anti"`. En Scala les alias canoniques sont `"leftsemi"` et `"leftanti"`.

---

## Gérer les colonnes dupliquées après jointure

```scala
// Problème : les deux DataFrames ont une colonne "id"
val result = df1.join(df2, df1("id") === df2("id"), "inner")
// → deux colonnes "id" → ambiguïté !

// Solution 1 : supprimer une après la jointure
val result1 = result.drop(df2("id"))

// Solution 2 : utiliser la clé en string (Spark déduplique automatiquement)
val result2 = df1.join(df2, "id", "inner")   // une seule colonne "id"

// Solution 3 : aliaser les DataFrames avant la jointure
val df1Alias = df1.alias("e")
val df2Alias = df2.alias("d")
val result3  = df1Alias.join(df2Alias, col("e.id") === col("d.id"))
val result4  = result3.select("e.id", "e.name", "d.dept_name")
```

---

## Points clés pour l'examen

> [ATTENTION] **Piège :** `leftsemi` et `leftanti` ne retournent **jamais** les colonnes du DataFrame de droite.

> [ATTENTION] **Piège :** Un `cross join` sur des tables de 1M lignes chacune produit **1 billion** de lignes !

> [OK] Pour les jointures de performance : utiliser `broadcast()` si un côté est petit (< 10 MB).
