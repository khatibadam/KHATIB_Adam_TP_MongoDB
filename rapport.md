# Rendu TP MongoDB - FraudShield Banking

---

## PARTIE 1 : Installation et Import du Dataset

---

### Question 1.1.1

**Requete :**
```javascript
use fraudshield_banking
```

**Resultat :**
```
switched to db fraudshield_banking
```

---

### Question 1.1.2

`mongoimport` avec les paramètres `--db`, `--collection`, `--type csv`, `--headerline` et `--file`.

---

### Question 1.1.3

**Requete :**
```bash
mongoimport --uri="mongodb://root:example@127.0.0.1:27017/?authSource=admin" --db fraudshield_banking --collection transactions --type csv --headerline --file "C:\Users\AdamK\Desktop\TSSR\Cours\MongoDB\TP_1\FraudShield_Banking_Data.csv"
```

**Resultat :**
```
2026-03-10T15:21:47.608+0100    connected to: mongodb://[**REDACTED**]@127.0.0.1:27017/?authSource=admin
2026-03-10T15:21:49.811+0100    50000 document(s) imported successfully. 0 document(s) failed to import.
```

50000 documents importés, les champs numériques sont en int32 mais les dates et les Yes/No restent en string. On corrige ca avec updateMany juste après.

**Verification :**
```javascript
db.transactions.countDocuments()
// 50000
```

```javascript
db.transactions.findOne()
```

```
{
  _id: ObjectId('69b2779b201a8ddc44c396e0'),
  Transaction_ID: 431438,
  Customer_ID: 24239,
  'Transaction_Amount (in Million)': 6,
  Transaction_Time: '10:54',
  Transaction_Date: '2025-03-08',
  Transaction_Type: 'POS',
  Merchant_ID: 97028,
  Merchant_Category: 'ATM',
  Transaction_Location: 'Singapore',
  Customer_Home_Location: 'Lahore',
  Distance_From_Home: 466,
  Device_ID: 363229,
  IP_Address: '231.92.159.84',
  Card_Type: 'Credit',
  'Account_Balance (in Million)': 30,
  Daily_Transaction_Count: 4,
  Weekly_Transaction_Count: 17,
  'Avg_Transaction_Amount (in Million)': 2,
  'Max_Transaction_Last_24h (in Million)': 4,
  Is_International_Transaction: 'Yes',
  Is_New_Merchant: 'Yes',
  Failed_Transaction_Count: 0,
  Unusual_Time_Transaction: 'No',
  Previous_Fraud_Count: 1,
  Fraud_Label: 'Normal'
}
```

---

Les colonnes du CSV ont des parentheses et des espaces dans les noms comme "Transaction_Amount in Million", c'est pas pratique dans les requetes donc je les renomme.

```javascript
db.transactions.updateMany({}, {
  $rename: {
    "Transaction_Amount (in Million)": "Transaction_Amount",
    "Account_Balance (in Million)": "Account_Balance",
    "Avg_Transaction_Amount (in Million)": "Avg_Transaction_Amount",
    "Max_Transaction_Last_24h (in Million)": "Max_Transaction_Last_24h"
  }
})
// { matchedCount: 50000, modifiedCount: 50000 }
```

---

### Question 1.2.1

**Requete :**
```javascript
db.transactions.find().limit(5)
```

**Resultat :**
```
{
  _id: ObjectId('69b2779b201a8ddc44c396e0'),
  Transaction_ID: 431438,
  Customer_ID: 24239,
  Transaction_Time: '10:54',
  Transaction_Date: '2025-03-08',
  Transaction_Type: 'POS',
  Merchant_ID: 97028,
  Merchant_Category: 'ATM',
  Transaction_Location: 'Singapore',
  Customer_Home_Location: 'Lahore',
  Distance_From_Home: 466,
  Device_ID: 363229,
  IP_Address: '231.92.159.84',
  Card_Type: 'Credit',
  Daily_Transaction_Count: 4,
  Weekly_Transaction_Count: 17,
  Is_International_Transaction: 'Yes',
  Is_New_Merchant: 'Yes',
  Failed_Transaction_Count: 0,
  Unusual_Time_Transaction: 'No',
  Previous_Fraud_Count: 1,
  Fraud_Label: 'Normal',
  Account_Balance: 30,
  Avg_Transaction_Amount: 2,
  Max_Transaction_Last_24h: 4,
  Transaction_Amount: 6
}
// ... 4 autres documents similaires
```

Chaque document c'est une transaction avec le client, le montant en millions, la date et l'heure en string, le type de transaction entre POS et ATM et Online, les infos du marchand et du device, le type de carte et le label Fraud ou Normal.

---

### Question 1.2.2

On fait deux updateMany par champ, un pour "Yes" vers true et un pour "No" vers false.

**Requete :**
```javascript
db.transactions.updateMany({ Is_International_Transaction: "Yes" }, { $set: { Is_International_Transaction: true } })
db.transactions.updateMany({ Is_International_Transaction: "No" }, { $set: { Is_International_Transaction: false } })

db.transactions.updateMany({ Is_New_Merchant: "Yes" }, { $set: { Is_New_Merchant: true } })
db.transactions.updateMany({ Is_New_Merchant: "No" }, { $set: { Is_New_Merchant: false } })

db.transactions.updateMany({ Unusual_Time_Transaction: "Yes" }, { $set: { Unusual_Time_Transaction: true } })
db.transactions.updateMany({ Unusual_Time_Transaction: "No" }, { $set: { Unusual_Time_Transaction: false } })
```

**Resultat :**
```
{ matchedCount: 24934, modifiedCount: 24934 }
{ matchedCount: 24939, modifiedCount: 24939 }
{ matchedCount: 25111, modifiedCount: 25111 }
```

---

## PARTIE 2 : Exploration et CRUD

---

### Question 2.1.1

**Requete :**
```javascript
db.transactions.countDocuments()
db.transactions.countDocuments({ Fraud_Label: "Fraud" })
db.transactions.countDocuments({ Fraud_Label: "Normal" })
```

**Resultat :**
```
50000
2423
47573
```

2423 fraudes sur 50000 donc 4.85%. Il y a 4 documents qui sont ni "Fraud" ni "Normal", surement des valeurs vides du CSV.

---

### Question 2.1.2

**Requete :**
```javascript
db.transactions.find().sort({ Transaction_Amount: -1 }).limit(1)
```

**Resultat :**
```
{
  Transaction_ID: 677479,
  Customer_ID: 72748,
  Transaction_Amount: '',
  Fraud_Label: 'Normal',
  ...
}
```

Le tri remonte un document avec un montant vide parce que les strings passent avant les nombres en tri descendant. J'ai refait avec un filtre :

```javascript
db.transactions.find({ Transaction_Amount: { $type: "number" } }).sort({ Transaction_Amount: -1 }).limit(1)
```

```
{
  Transaction_ID: 902451,
  Customer_ID: 77250,
  Transaction_Amount: 9,
  Fraud_Label: 'Normal',
  Transaction_Date: '2025-01-17',
  Transaction_Type: 'ATM',
  Merchant_Category: 'ATM',
  Transaction_Location: 'Singapore',
  ...
}
```

La plus grosse transaction fait 9 millions et c'est pas une fraude.

---

### Question 2.1.3

**Requete :**
```javascript
db.transactions.aggregate([
  { $group: { _id: "$Customer_ID", nombre_transactions: { $sum: 1 } } },
  { $sort: { nombre_transactions: -1 } },
  { $limit: 10 },
  { $project: { _id: 0, Customer_ID: "$_id", nombre_transactions: 1 } }
])
```

**Resultat :**
```
{ nombre_transactions: 10, Customer_ID: '' }
{ nombre_transactions: 6, Customer_ID: 43223 }
{ nombre_transactions: 6, Customer_ID: 15303 }
{ nombre_transactions: 6, Customer_ID: 88220 }
{ nombre_transactions: 6, Customer_ID: 10985 }
{ nombre_transactions: 5, Customer_ID: 12558 }
{ nombre_transactions: 5, Customer_ID: 16498 }
{ nombre_transactions: 5, Customer_ID: 63233 }
{ nombre_transactions: 5, Customer_ID: 92323 }
{ nombre_transactions: 5, Customer_ID: 92323 }
```

---

### Question 2.2.1

**Requete :**
```javascript
db.transactions.countDocuments({
  Transaction_Amount: { $gt: 5 },
  Is_International_Transaction: true,
  Card_Type: "Credit",
  Previous_Fraud_Count: { $gt: 0 }
})

db.transactions.countDocuments({
  Transaction_Amount: { $gt: 5 },
  Is_International_Transaction: true,
  Card_Type: "Credit",
  Previous_Fraud_Count: { $gt: 0 },
  Fraud_Label: "Fraud"
})
```

**Resultat :**
```
2667
173
```

2667 transactions correspondent, 173 sont des fraudes soit 173 / 2667 * 100 = 6.49%.

---

### Question 2.2.2

**Requete :**
```javascript
db.transactions.countDocuments({
  Unusual_Time_Transaction: true,
  Distance_From_Home: { $gt: 100 }
})

db.transactions.countDocuments({
  Unusual_Time_Transaction: true,
  Distance_From_Home: { $gt: 100 },
  Fraud_Label: "Fraud"
})
```

**Resultat :**
```
20734
1231
```

1231 frauduleuses sur 20734 soit 5.94%.

---

### Question 2.2.3

**Requete :**
```javascript
db.transactions.find(
  { Merchant_Category: { $in: ["Clothing", "Electronics", "Restaurant"] } },
  { Transaction_ID: 1, Transaction_Amount: 1, Merchant_Category: 1, Fraud_Label: 1, _id: 0 }
).limit(10)
```

**Resultat :**
```
{ Transaction_ID: 223410, Transaction_Amount: 3, Merchant_Category: 'Electronics', Fraud_Label: 'Normal' }
{ Transaction_ID: 414637, Transaction_Amount: 7, Merchant_Category: 'Electronics', Fraud_Label: 'Normal' }
{ Transaction_ID: 710739, Transaction_Amount: 1, Merchant_Category: 'Electronics', Fraud_Label: 'Normal' }
{ Transaction_ID: 627496, Transaction_Amount: 5, Merchant_Category: 'Electronics', Fraud_Label: 'Normal' }
{ Transaction_ID: 108171, Transaction_Amount: 2, Merchant_Category: 'Restaurant', Fraud_Label: 'Normal' }
{ Transaction_ID: 731894, Transaction_Amount: 8, Merchant_Category: 'Electronics', Fraud_Label: 'Normal' }
{ Transaction_ID: 952375, Transaction_Amount: 4, Merchant_Category: 'Restaurant', Fraud_Label: 'Normal' }
{ Transaction_ID: 862985, Transaction_Amount: 6, Merchant_Category: 'Clothing', Fraud_Label: 'Normal' }
{ Transaction_ID: 223921, Transaction_Amount: 1, Merchant_Category: 'Electronics', Fraud_Label: 'Normal' }
{ Transaction_ID: 806879, Transaction_Amount: 9, Merchant_Category: 'Clothing', Fraud_Label: 'Normal' }
```

---

### Question 2.3.1

**Requete :**
```javascript
db.transactions.updateMany(
  { Customer_ID: 24239, Transaction_Date: "2025-01-15", Fraud_Label: "Fraud" },
  { $set: { Fraud_Label: "Normal" } }
)
```

**Resultat :**
```
{ matchedCount: 0, modifiedCount: 0 }
```

Le client 24239 n'avait pas de transaction frauduleuse le 15 janvier 2025, donc rien a corriger.

---

### Question 2.3.2

**Requete :**
```javascript
// Tout le monde a LOW
db.transactions.updateMany({}, { $set: { risk_level: "LOW" } })

// Ceux qui matchent passent a MEDIUM
db.transactions.updateMany(
  { $or: [
    { Transaction_Amount: { $gt: 5 } },
    { Is_International_Transaction: true },
    { Failed_Transaction_Count: { $gt: 3 } }
  ]},
  { $set: { risk_level: "MEDIUM" } }
)

// Les plus risques passent a HIGH
db.transactions.updateMany(
  { $or: [
    { Transaction_Amount: { $gt: 10 } },
    { Previous_Fraud_Count: { $gt: 2 } },
    { Distance_From_Home: { $gt: 500 } }
  ]},
  { $set: { risk_level: "HIGH" } }
)
```

**Resultat :**
```
{ matchedCount: 50000, modifiedCount: 50000 }
{ matchedCount: 33696, modifiedCount: 33696 }
{ matchedCount: 8185, modifiedCount: 8185 }
```

---

### Question 2.3.3

**Requete :**
```javascript
db.transactions.updateMany(
  { Transaction_Date: { $gte: "2025-01-01", $lte: "2025-01-31" } },
  { $set: { IP_Address: "ANONYMIZED" } }
)
```

**Resultat :**
```
{ matchedCount: 12691, modifiedCount: 12691 }
```

Comme Transaction_Date est en string "yyyy-mm-dd" la comparaison alphabétique marche dans le bon ordre donc $gte et $lte suffisent.

---

### Question 2.4.1

**Requete :**
```javascript
// Copier dans archive
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud", Failed_Transaction_Count: { $gte: 2 } } },
  { $out: "archive_transactions" }
])

db.archive_transactions.countDocuments()

// Supprimer de la collection principale
db.transactions.deleteMany({ Fraud_Label: "Fraud", Failed_Transaction_Count: { $gte: 2 } })

// Verifier
db.transactions.countDocuments({ Fraud_Label: "Fraud", Failed_Transaction_Count: { $gte: 2 } })
```

**Resultat :**
```
773
{ deletedCount: 773 }
0
```

Je reinjecte les données archivées pour la suite du TP :

```javascript
db.archive_transactions.find().forEach(function(doc) {
  delete doc._id;
  db.transactions.insertOne(doc);
})
// db.transactions.countDocuments() → 50000
```

---

## PARTIE 3 : Requetes Avancees

---

### Question 3.1.1

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud" } },
  { $project: { heure: { $toInt: { $substr: ["$Transaction_Time", 0, 2] } } } },
  { $group: { _id: "$heure", nombre_fraudes: { $sum: 1 } } },
  { $sort: { nombre_fraudes: -1 } }
])
```

**Resultat :**
```
{ _id: 21, nombre_fraudes: 118 }
{ _id: 13, nombre_fraudes: 114 }
{ _id: 7, nombre_fraudes: 113 }
{ _id: 23, nombre_fraudes: 112 }
...
```

---

### Question 3.1.2

**Requete :**
```javascript
db.transactions.aggregate([
  { $group: {
    _id: { customer: "$Customer_ID", date: "$Transaction_Date" },
    count: { $sum: 1 },
    has_fraud: { $max: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
  }},
  { $match: { count: { $gt: 10 }, has_fraud: 1 } },
  { $project: { _id: 0, Customer_ID: "$_id.customer", nombre_transactions: "$count" } }
])
```

**Resultat :**
```
// Aucun resultat
```

Le maximum de transactions par client par jour c'est 2 dans le dataset, donc aucun resultat possible.

---

### Question 3.2.1

**Requete :**
```javascript
db.transactions.aggregate([
  { $group: {
    _id: "$Transaction_Location",
    total: { $sum: 1 },
    fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
  }},
  { $project: {
    _id: 0, localisation: "$_id", total: 1, fraudes: 1,
    taux_fraude: { $round: [{ $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }, 2] }
  }},
  { $sort: { taux_fraude: -1 } },
  { $limit: 5 }
])
```

**Resultat :**
```
{ total: 5025, fraudes: 256, localisation: 'Singapore', taux_fraude: 5.09 }
{ total: 4986, fraudes: 253, localisation: 'Bangkok', taux_fraude: 5.07 }
{ total: 4896, fraudes: 241, localisation: 'London', taux_fraude: 4.92 }
{ total: 5017, fraudes: 246, localisation: 'Faisalabad', taux_fraude: 4.9 }
{ total: 5071, fraudes: 247, localisation: 'Kuala Lumpur', taux_fraude: 4.87 }
```

---

### Question 3.2.2

**Requete :**
```javascript
db.transactions.countDocuments({
  $expr: { $ne: ["$Transaction_Location", "$Customer_Home_Location"] },
  Distance_From_Home: { $gt: 200 }
})

db.transactions.countDocuments({
  $expr: { $ne: ["$Transaction_Location", "$Customer_Home_Location"] },
  Distance_From_Home: { $gt: 200 },
  Fraud_Label: "Fraud"
})
```

**Resultat :**
```
30027
1426
```

Ca fait 1426 fraudes sur 30027 soit 4.75%.

---

### Question 3.3.1

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud" } },
  { $group: {
    _id: "$Merchant_ID",
    montant_total_fraudes: { $sum: "$Transaction_Amount" },
    nombre_fraudes: { $sum: 1 },
    montant_moyen: { $avg: "$Transaction_Amount" }
  }},
  { $sort: { montant_total_fraudes: -1 } },
  { $limit: 10 },
  { $project: { _id: 0, Merchant_ID: "$_id", montant_total_fraudes: 1, nombre_fraudes: 1, montant_moyen: { $round: ["$montant_moyen", 2] } } }
])
```

**Resultat :**
```
{ montant_total_fraudes: 17, nombre_fraudes: 2, Merchant_ID: 86181, montant_moyen: 8.5 }
{ montant_total_fraudes: 17, nombre_fraudes: 2, Merchant_ID: 15527, montant_moyen: 8.5 }
{ montant_total_fraudes: 16, nombre_fraudes: 2, Merchant_ID: 35158, montant_moyen: 8 }
{ montant_total_fraudes: 15, nombre_fraudes: 2, Merchant_ID: 88714, montant_moyen: 7.5 }
{ montant_total_fraudes: 14, nombre_fraudes: 2, Merchant_ID: 44566, montant_moyen: 7 }
{ montant_total_fraudes: 14, nombre_fraudes: 2, Merchant_ID: 65819, montant_moyen: 7 }
{ montant_total_fraudes: 13, nombre_fraudes: 2, Merchant_ID: 43826, montant_moyen: 6.5 }
{ montant_total_fraudes: 12, nombre_fraudes: 2, Merchant_ID: 28999, montant_moyen: 6 }
{ montant_total_fraudes: 12, nombre_fraudes: 2, Merchant_ID: 79424, montant_moyen: 6 }
{ montant_total_fraudes: 12, nombre_fraudes: 2, Merchant_ID: 95335, montant_moyen: 6 }
```

---

### Question 3.3.2

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Merchant_Category: { $ne: "" }, Card_Type: { $in: ["Credit", "Debit"] } } },
  { $group: {
    _id: "$Merchant_Category",
    credit: { $sum: { $cond: [{ $eq: ["$Card_Type", "Credit"] }, 1, 0] } },
    debit: { $sum: { $cond: [{ $eq: ["$Card_Type", "Debit"] }, 1, 0] } }
  }},
  { $addFields: { ratio: { $round: [{ $divide: ["$credit", "$debit"] }, 2] } } },
  { $sort: { ratio: -1 } }
])
```

**Resultat :**
```
{ _id: 'Grocery', credit: 4153, debit: 4099, ratio: 1.01 }
{ _id: 'Electronics', credit: 4100, debit: 4116, ratio: 1 }
{ _id: 'Restaurant', credit: 4236, debit: 4246, ratio: 1 }
{ _id: 'Fuel', credit: 4152, debit: 4204, ratio: 0.99 }
{ _id: 'Clothing', credit: 4110, debit: 4171, ratio: 0.99 }
{ _id: 'ATM', credit: 4136, debit: 4265, ratio: 0.97 }
```

---

### Question 3.4.1

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Avg_Transaction_Amount: { $gt: 0 } } },
  { $addFields: { ratio: { $divide: ["$Transaction_Amount", "$Avg_Transaction_Amount"] } } },
  { $match: { ratio: { $gt: 3 } } },
  { $group: {
    _id: null,
    total: { $sum: 1 },
    fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
  }},
  { $project: { _id: 0, total: 1, fraudes: 1, taux_fraude: { $round: [{ $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }, 2] } } }
])
```

**Resultat :**
```
{ total: 10093, fraudes: 510, taux_fraude: 5.05 }
```

5.05% contre 4.85% global.

---

### Question 3.4.2

**Requete :**
```javascript
db.transactions.countDocuments({ Is_New_Merchant: true, Is_International_Transaction: true })

db.transactions.countDocuments({ Is_New_Merchant: true, Is_International_Transaction: true, Fraud_Label: "Fraud" })
```

**Resultat :**
```
12598
804
```

6.38% de fraude, c'est au dessus du global de 4.85%. Ca se tient parce qu'un fraudeur utilise souvent un marchand inconnu a l'etranger.

---

### Question 3.4.3

**Requete :**
```javascript
db.transactions.aggregate([
  { $addFields: {
    suspicion_score: { $sum: [
      { $cond: [{ $gt: ["$Transaction_Amount", { $multiply: ["$Avg_Transaction_Amount", 2] }] }, 1, 0] },
      { $cond: [{ $eq: ["$Unusual_Time_Transaction", true] }, 1, 0] },
      { $cond: [{ $eq: ["$Is_New_Merchant", true] }, 1, 0] },
      { $cond: [{ $eq: ["$Is_International_Transaction", true] }, 1, 0] },
      { $cond: [{ $gt: ["$Distance_From_Home", 100] }, 1, 0] },
      { $cond: [{ $gt: ["$Daily_Transaction_Count", 5] }, 1, 0] }
    ]}
  }},
  { $match: { suspicion_score: { $gte: 3 } } },
  { $group: {
    _id: null,
    total: { $sum: 1 },
    fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
  }},
  { $project: { _id: 0, total: 1, fraudes: 1, taux_fraude: { $round: [{ $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }, 2] } } }
])
```

**Resultat :**
```
{ total: 32804, fraudes: 1814, taux_fraude: 5.53 }
```

32804 transactions remplissent au moins 3 critères sur 6, taux de fraude de 5.53%. J'ai testé avec 4 d'abord mais ca changeait pas grand chose au taux.

---

## PARTIE 4 : Indexation et Performance

---

### Question 4.1.1

On a converti les Yes/No en booléens en partie 1 donc j'utilise true au lieu de "Yes".

**Requete :**
```javascript
db.transactions.find({
  Transaction_Amount: { $gt: 5 },
  Fraud_Label: "Fraud",
  Is_International_Transaction: true
}).explain("executionStats")
```

**Resultat :**
```
executionTimeMillis: 48
totalDocsExamined: 50000
nReturned: 679
stage: COLLSCAN
```

![explain sans index COLLSCAN](screenshots/screenshot_1.png)

---

### Question 4.1.2

**Recherche par client :**
```javascript
db.transactions.find({ Customer_ID: 24239 }).explain("executionStats")
// executionTimeMillis: 38, totalDocsExamined: 50000, nReturned: 1, COLLSCAN
```

**Fraudes récentes :**
```javascript
db.transactions.find({ Fraud_Label: "Fraud", Transaction_Date: { $gte: "2025-03-01" } }).explain("executionStats")
// executionTimeMillis: 42, totalDocsExamined: 50000, nReturned: 1210, COLLSCAN
```

**Par marchand :**
```javascript
db.transactions.find({ Merchant_ID: 97028, Transaction_Amount: { $gt: 5 } }).explain("executionStats")
// executionTimeMillis: 34, totalDocsExamined: 50000, nReturned: 2, COLLSCAN
```

Les trois requetes font un COLLSCAN sur 50000 documents meme quand elles retournent tres peu de resultats.

---

### Question 4.2.1

**Requete :**
```javascript
db.transactions.createIndex({ Fraud_Label: 1 })
```

**Resultat après ré-exécution de la requete 4.1.1 :**
```
executionTimeMillis: 7
totalDocsExamined: 2423
nReturned: 679
stage: FETCH → IXSCAN
```

Ca passe de 48ms à 7ms. Avec l'index mongodb scanne que les 2423 fraudes au lieu des 50000.

![explain avec index Fraud_Label IXSCAN](screenshots/screenshot_2.png)
![explain stats FETCH apres index](screenshots/screenshot_2_bis.png)

---

### Question 4.2.2

**Requete :**
```javascript
db.transactions.createIndex({ Customer_ID: 1, Transaction_Date: -1, Transaction_Amount: 1 })
```

J'ai mis Customer_ID en premier parce que c'est l'egalite, puis Transaction_Date en -1 pour le tri et Transaction_Amount a la fin pour le range.

**Resultat :**
```javascript
db.transactions.find({
  Customer_ID: 24239,
  Transaction_Amount: { $gte: 1, $lte: 10 }
}).sort({ Transaction_Date: -1 }).explain("executionStats")
```

```
executionTimeMillis: 1
totalDocsExamined: 1
nReturned: 1
stage: FETCH → IXSCAN
```

![explain index compose IXSCAN](screenshots/screenshot_3.png)

---

### Question 4.2.3

**Requete :**
```javascript
db.transactions.createIndex({ Transaction_Location: 1, Merchant_Category: 1 })
```

**Resultat :**
```javascript
db.transactions.find({ Transaction_Location: "Singapore", Merchant_Category: "Electronics" }).explain("executionStats")
// executionTimeMillis: 2, totalDocsExamined: 837, nReturned: 837, IXSCAN
```

---

### Question 4.2.4

**Requete :**
```javascript
db.transactions.createIndex({ IP_Address: 1 }, { unique: true })
```

**Resultat :**
```
E11000 duplicate key error collection: fraudshield_banking.transactions
dup key: { IP_Address: "" }
```

Ca marche pas parce qu'on a plein de "ANONYMIZED" a cause de la question 2.3.3 et des strings vides. Pour que ca passe il faudrait un index unique partiel qui exclut les vides et les ANONYMIZED.

---

### Question 4.3.1

**Requete :**
```javascript
db.transactions.createIndex(
  { Transaction_Amount: 1 },
  { partialFilterExpression: { Fraud_Label: "Fraud", Transaction_Amount: { $gt: 1 } } }
)
```

Un index partiel n'indexe que les documents qui matchent le filtre, ca prend donc beaucoup moins de place. Celui-ci fait 32 Ko au lieu de 250+ Ko pour un index complet.

---

### Question 4.3.2

**Requete :**
```javascript
db.transactions.createIndex({ Previous_Fraud_Count: 1 }, { sparse: true })
```

Un index sparse n'inclut pas les documents ou le champ n'existe pas. Sauf que dans notre cas tous les documents ont Previous_Fraud_Count donc ca ne change rien. C'etait surtout pour tester la syntaxe.

---

### Question 4.3.3

**Requete :**
```javascript
db.transactions.getIndexes()
db.transactions.stats().indexSizes
```

**Resultat :**
```
_id_: 524288
Fraud_Label_1: 253952
Customer_ID_1_Transaction_Date_-1_Transaction_Amount_1: 1261568
Transaction_Location_1_Merchant_Category_1: 339968
Transaction_Amount_1: 32768 (partiel)
Previous_Fraud_Count_1: 331776 (sparse)
```

L'index sparse Previous_Fraud_Count_1 fait 330 Ko pour rien vu que tous les documents ont ce champ.

---

### Question 4.4.1

L'index composé de la question 4.2.2 couvre deja les trois champs qu'on projette donc ca devrait etre une covered query.

**Requete :**
```javascript
db.transactions.find(
  { Customer_ID: 24239 },
  { Customer_ID: 1, Transaction_Amount: 1, Transaction_Date: 1, _id: 0 }
).explain("executionStats")
```

**Resultat :**
```
executionTimeMillis: 0
totalDocsExamined: 0
totalKeysExamined: 1
nReturned: 1
stage: PROJECTION_COVERED → IXSCAN
```

totalDocsExamined a 0, mongodb n'a meme pas besoin de lire les documents car tout est dans l'index.

![explain covered query PROJECTION_COVERED](screenshots/screenshot_4.png)

---

## PARTIE 5 : Agregation et Analyse Avancee

---

### Question 5.1.1

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Card_Type: { $ne: "" } } },
  { $group: {
    _id: "$Card_Type",
    montant_total: { $sum: "$Transaction_Amount" },
    montant_moyen: { $avg: "$Transaction_Amount" },
    nombre_transactions: { $sum: 1 }
  }},
  { $sort: { montant_total: -1 } },
  { $project: { _id: 0, type_carte: "$_id", montant_total: 1, montant_moyen: { $round: ["$montant_moyen", 2] }, nombre_transactions: 1 } }
])
```

**Resultat :**
```
{ montant_total: 125167, nombre_transactions: 25101, type_carte: 'Debit', montant_moyen: 4.99 }
{ montant_total: 124769, nombre_transactions: 24887, type_carte: 'Credit', montant_moyen: 5.01 }
```

---

### Question 5.1.2

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Merchant_Category: { $ne: "" } } },
  { $group: {
    _id: "$Merchant_Category",
    total: { $sum: 1 },
    fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
    montant_moyen_fraudes: { $avg: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, "$Transaction_Amount", null] } }
  }},
  { $addFields: { taux_fraude: { $round: [{ $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }, 2] } } },
  { $match: { taux_fraude: { $gt: 10 } } },
  { $sort: { taux_fraude: -1 } }
])
```

**Resultat :**
```
// Aucun resultat
```

Aucune categorie au dessus de 10%, la plus haute c'est Restaurant a 5.03%.

---

### Question 5.1.3

**Requete :**
```javascript
db.transactions.aggregate([
  { $group: {
    _id: "$Customer_ID",
    solde: { $max: "$Account_Balance" },
    nombre_transactions: { $sum: 1 }
  }},
  { $sort: { solde: -1 } },
  { $limit: 20 },
  { $project: { _id: 0, Customer_ID: "$_id", solde: 1, nombre_transactions: 1 } }
])
```

**Resultat :**
```
{ solde: 39, nombre_transactions: 1, Customer_ID: 97591 }
{ solde: 39, nombre_transactions: 2, Customer_ID: 86438 }
{ solde: 39, nombre_transactions: 2, Customer_ID: 43824 }
// ... les 20 sont tous a 39 millions
```

---

### Question 5.2.1

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Transaction_Date: { $ne: "" } } },
  { $addFields: { date_parsed: { $dateFromString: { dateString: "$Transaction_Date", format: "%Y-%m-%d" } } } },
  { $group: {
    _id: { $isoWeek: "$date_parsed" },
    total_transactions: { $sum: 1 },
    fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
    montant_total_fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, "$Transaction_Amount", 0] } }
  }},
  { $addFields: { montant_moyen_fraude: { $cond: [{ $gt: ["$fraudes", 0] }, { $round: [{ $divide: ["$montant_total_fraudes", "$fraudes"] }, 2] }, 0] } } },
  { $sort: { _id: 1 } },
  { $project: { _id: 0, semaine: "$_id", total_transactions: 1, fraudes: 1, montant_total_fraudes: 1, montant_moyen_fraude: 1 } }
])
```

**Resultat :**
```
{ semaine: 1, total_transactions: 2015, fraudes: 112, montant_total_fraudes: 543, montant_moyen_fraude: 4.85 }
{ semaine: 2, total_transactions: 2886, fraudes: 154, montant_total_fraudes: 772, montant_moyen_fraude: 5.01 }
{ semaine: 3, total_transactions: 2933, fraudes: 143, montant_total_fraudes: 724, montant_moyen_fraude: 5.06 }
{ semaine: 4, total_transactions: 2846, fraudes: 129, montant_total_fraudes: 680, montant_moyen_fraude: 5.27 }
{ semaine: 5, total_transactions: 2860, fraudes: 124, montant_total_fraudes: 574, montant_moyen_fraude: 4.63 }
// ... 18 semaines au total
```

---

### Question 5.2.2

**Requete :**
```javascript
db.transactions.aggregate([
  { $addFields: {
    groupe: { $switch: {
      branches: [
        { case: { $eq: ["$Previous_Fraud_Count", 0] }, then: "Propres" },
        { case: { $lte: ["$Previous_Fraud_Count", 2] }, then: "Risque modere" }
      ],
      default: "Haut risque"
    }}
  }},
  { $group: {
    _id: "$groupe",
    total: { $sum: 1 },
    fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
    montant_moyen: { $avg: "$Transaction_Amount" }
  }},
  { $addFields: {
    taux_fraude: { $round: [{ $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }, 2] },
    montant_moyen: { $round: ["$montant_moyen", 2] }
  }},
  { $sort: { _id: 1 } }
])
```

**Resultat :**
```
{ _id: 'Propres', total: 24992, fraudes: 1185, montant_moyen: 5.01, taux_fraude: 4.74 }
{ _id: 'Risque modere', total: 25005, fraudes: 1238, montant_moyen: 4.99, taux_fraude: 4.95 }
```

Il n'y a que 2 groupes parce que Previous_Fraud_Count depasse pas 2 dans le dataset.

---

### Question 5.2.3

**Requete :**
```javascript
db.transactions.aggregate([
  { $addFields: { heure: { $toInt: { $substr: ["$Transaction_Time", 0, 2] } } } },
  { $group: {
    _id: "$heure",
    total: { $sum: 1 },
    fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
  }},
  { $addFields: { ratio_fraude: { $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] } } },
  { $sort: { ratio_fraude: -1 } },
  { $limit: 5 }
])
```

**Resultat :**
```
{ _id: 21, total: 2045, fraudes: 118, ratio_fraude: 5.770... }
{ _id: 13, total: 2081, fraudes: 114, ratio_fraude: 5.478... }
{ _id: 23, total: 2088, fraudes: 112, ratio_fraude: 5.363... }
{ _id: 8, total: 2032, fraudes: 107, ratio_fraude: 5.265... }
{ _id: 14, total: 2108, fraudes: 111, ratio_fraude: 5.265... }
```

---

### Question 5.3.1

**Creation de la collection marchands :**
```javascript
db.merchants.insertMany([
  { Merchant_ID: 96715, nom: "ElectroParis", adresse: "Paris, 12 rue Rivoli", categorie: "Electronics", date_ouverture: new Date("2020-01-01") },
  { Merchant_ID: 95981, nom: "FashionLondon", adresse: "London, 45 Oxford Street", categorie: "Clothing", date_ouverture: new Date("2020-02-01") },
  { Merchant_ID: 50678, nom: "RestoBistro", adresse: "New York, 8th Avenue", categorie: "Restaurant", date_ouverture: new Date("2020-03-01") },
  { Merchant_ID: 19716, nom: "FreshMarket", adresse: "Tokyo, Shibuya 3-12", categorie: "Grocery", date_ouverture: new Date("2021-04-01") },
  { Merchant_ID: 92541, nom: "PetrolPlus", adresse: "Dubai, Sheikh Zayed Road", categorie: "Fuel", date_ouverture: new Date("2021-05-01") },
  { Merchant_ID: 48263, nom: "CashPoint", adresse: "Singapore, Orchard Road", categorie: "ATM", date_ouverture: new Date("2021-06-01") },
  { Merchant_ID: 85954, nom: "BioMarkt", adresse: "Berlin, Alexanderplatz", categorie: "Grocery", date_ouverture: new Date("2022-07-01") },
  { Merchant_ID: 44621, nom: "TechMadrid", adresse: "Madrid, Gran Via 22", categorie: "Electronics", date_ouverture: new Date("2022-08-01") },
  { Merchant_ID: 33744, nom: "OzGrill", adresse: "Sydney, George Street", categorie: "Restaurant", date_ouverture: new Date("2022-09-01") },
  { Merchant_ID: 43064, nom: "MapleGas", adresse: "Toronto, Yonge Street", categorie: "Fuel", date_ouverture: new Date("2023-10-01") }
])
```

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud", Merchant_ID: { $in: [96715, 95981, 50678, 19716, 92541, 48263, 85954, 44621, 33744, 43064] } } },
  { $lookup: { from: "merchants", localField: "Merchant_ID", foreignField: "Merchant_ID", as: "merchant_info" } },
  { $unwind: "$merchant_info" },
  { $project: { _id: 0, Transaction_ID: 1, Transaction_Amount: 1, Merchant_ID: 1, "merchant_info.nom": 1, "merchant_info.adresse": 1 } },
  { $limit: 5 }
])
```

**Resultat :**
```
{ Transaction_ID: 111567, Merchant_ID: 19716, Transaction_Amount: 1, merchant_info: { nom: 'FreshMarket', adresse: 'Tokyo, Shibuya 3-12' } }
{ Transaction_ID: 463841, Merchant_ID: 92541, Transaction_Amount: 6, merchant_info: { nom: 'PetrolPlus', adresse: 'Dubai, Sheikh Zayed Road' } }
{ Transaction_ID: 820567, Merchant_ID: 48263, Transaction_Amount: 3, merchant_info: { nom: 'CashPoint', adresse: 'Singapore, Orchard Road' } }
{ Transaction_ID: 819250, Merchant_ID: 33744, Transaction_Amount: 9, merchant_info: { nom: 'OzGrill', adresse: 'Sydney, George Street' } }
{ Transaction_ID: 737184, Merchant_ID: 43064, Transaction_Amount: 2, merchant_info: { nom: 'MapleGas', adresse: 'Toronto, Yonge Street' } }
```

---

### Question 5.3.2

**Creation de la collection clients :**
```javascript
db.customers.insertMany([
  { Customer_ID: 10005, prenom: "Ahmed", nom: "Dupont", email: "ahmed.dupont@mail.com", date_inscription: new Date("2019-01-15") },
  { Customer_ID: 10009, prenom: "Marie", nom: "Martin", email: "marie.martin@mail.com", date_inscription: new Date("2019-02-15") },
  { Customer_ID: 10013, prenom: "Jean", nom: "Garcia", email: "jean.garcia@mail.com", date_inscription: new Date("2019-03-15") },
  { Customer_ID: 10015, prenom: "Fatima", nom: "Lee", email: "fatima.lee@mail.com", date_inscription: new Date("2020-04-15") },
  { Customer_ID: 10018, prenom: "Carlos", nom: "Singh", email: "carlos.singh@mail.com", date_inscription: new Date("2020-05-15") },
  { Customer_ID: 10023, prenom: "Lin", nom: "Chen", email: "lin.chen@mail.com", date_inscription: new Date("2020-06-15") },
  { Customer_ID: 10024, prenom: "Sofia", nom: "Morel", email: "sofia.morel@mail.com", date_inscription: new Date("2021-07-15") },
  { Customer_ID: 10025, prenom: "Raj", nom: "Ali", email: "raj.ali@mail.com", date_inscription: new Date("2021-08-15") },
  { Customer_ID: 10030, prenom: "Emma", nom: "Schmidt", email: "emma.schmidt@mail.com", date_inscription: new Date("2021-09-15") },
  { Customer_ID: 10031, prenom: "Omar", nom: "Tanaka", email: "omar.tanaka@mail.com", date_inscription: new Date("2022-10-15") }
])
```

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Customer_ID: { $in: [10005, 10009, 10013, 10015, 10018, 10023, 10024, 10025, 10030, 10031] } } },
  { $group: {
    _id: "$Customer_ID",
    total_transactions: { $sum: 1 },
    fraudes_historiques: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
    montant_moyen: { $avg: "$Transaction_Amount" },
    max_previous_fraud: { $max: "$Previous_Fraud_Count" }
  }},
  { $lookup: { from: "customers", localField: "_id", foreignField: "Customer_ID", as: "info_client" } },
  { $unwind: "$info_client" },
  { $addFields: {
    score_risque: { $sum: [
      { $multiply: ["$fraudes_historiques", 20] },
      { $multiply: ["$max_previous_fraud", 10] },
      { $cond: [{ $gt: ["$montant_moyen", 5] }, 15, 0] }
    ]}
  }},
  { $project: {
    _id: 0, Customer_ID: "$_id",
    prenom: "$info_client.prenom", nom: "$info_client.nom",
    total_transactions: 1, fraudes_historiques: 1,
    montant_moyen: { $round: ["$montant_moyen", 2] }, score_risque: 1
  }},
  { $sort: { score_risque: -1 } }
])
```

**Resultat :**
```
{ Customer_ID: 10030, prenom: 'Emma', nom: 'Schmidt', total_transactions: 1, fraudes_historiques: 1, montant_moyen: 3, score_risque: 30 }
{ Customer_ID: 10025, prenom: 'Raj', nom: 'Ali', total_transactions: 1, fraudes_historiques: 0, montant_moyen: 8, score_risque: 25 }
{ Customer_ID: 10013, prenom: 'Jean', nom: 'Garcia', total_transactions: 1, fraudes_historiques: 0, montant_moyen: 9, score_risque: 25 }
{ Customer_ID: 10024, prenom: 'Sofia', nom: 'Morel', total_transactions: 2, fraudes_historiques: 0, montant_moyen: 6, score_risque: 25 }
{ Customer_ID: 10031, prenom: 'Omar', nom: 'Tanaka', total_transactions: 2, fraudes_historiques: 0, montant_moyen: 7.5, score_risque: 25 }
// ...
```

---

### Question 5.4.1

**Requete :**
```javascript
db.transactions.aggregate([
  { $addFields: {
    score_suspicion: { $sum: [
      { $cond: [{ $eq: ["$Is_International_Transaction", true] }, 3, 0] },
      { $cond: [{ $eq: ["$Is_New_Merchant", true] }, 2, 0] },
      { $cond: [{ $eq: ["$Unusual_Time_Transaction", true] }, 2, 0] },
      { $cond: [{ $gt: ["$Distance_From_Home", 100] }, 2, 0] },
      { $cond: [{ $gt: ["$Transaction_Amount", { $multiply: ["$Avg_Transaction_Amount", 2] }] }, 3, 0] },
      { $cond: [{ $gt: ["$Failed_Transaction_Count", 0] }, 2, 0] }
    ]}
  }},
  { $sort: { score_suspicion: -1 } },
  { $limit: 50 },
  { $group: {
    _id: null,
    total: { $sum: 1 },
    fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
    score_max: { $max: "$score_suspicion" }
  }}
])
```

**Resultat :**
```
{ total: 50, fraudes: 2, score_max: 14 }
```

Sur les 50 scores les plus eleves, seulement 2 sont de vraies fraudes. Le scoring seul est pas terrible pour detecter les fraudes.

---

### Question 5.4.2

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Transaction_Date: { $ne: "" } } },
  { $group: {
    _id: "$Transaction_Date",
    nombre_transactions: { $sum: 1 },
    nombre_fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
    montant_total_fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, "$Transaction_Amount", 0] } },
  }},
  { $addFields: {
    taux_fraude: { $round: [{ $multiply: [{ $divide: ["$nombre_fraudes", "$nombre_transactions"] }, 100] }, 2] }
  }},
  { $sort: { _id: 1 } },
  { $project: {
    _id: 0, date: "$_id",
    nombre_transactions: 1, nombre_fraudes: 1,
    montant_total_fraudes: 1, taux_fraude: 1
  }},
  { $out: "daily_fraud_stats" }
])
```

**Resultat :**
```
// 121 documents créés
{ date: '2025-05-01', nombre_transactions: 413, nombre_fraudes: 16, montant_total_fraudes: 72, taux_fraude: 3.87 }
{ date: '2025-04-30', nombre_transactions: 391, nombre_fraudes: 21, montant_total_fraudes: 100, taux_fraude: 5.37 }
```

Pour mettre a jour tous les jours on pourrait utiliser $merge au lieu de $out comme ca on ecrase pas les anciens jours.

---

## PARTIE 6 : Requetes Expertes et Optimisation

---

### Question 6.1.1

**Requete :**
```javascript
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud", Transaction_Date: { $ne: "" } } },
  { $addFields: { date_parsed: { $dateFromString: { dateString: "$Transaction_Date", format: "%Y-%m-%d" } } } },
  { $sort: { Customer_ID: 1, date_parsed: 1 } },
  { $group: {
    _id: "$Customer_ID",
    fraud_dates: { $push: "$date_parsed" },
    fraud_amounts: { $push: "$Transaction_Amount" },
    total_fraudes: { $sum: 1 }
  }},
  { $match: { total_fraudes: { $gte: 3 } } },
  { $project: {
    _id: 0, Customer_ID: "$_id",
    nombre_fraudes: "$total_fraudes",
    montant_total: { $sum: "$fraud_amounts" },
    dates_fraudes: { $map: { input: "$fraud_dates", as: "d", in: { $dateToString: { format: "%Y-%m-%d", date: "$$d" } } } }
  }}
])
```

**Resultat :**
```
{ Customer_ID: 41045, nombre_fraudes: 3, montant_total: 13, dates_fraudes: ['2025-01-26', '2025-02-24', '2025-03-25'] }
```

Il n'y a qu'un seul client avec 3 fraudes mais elles sont espacées d'un mois a chaque fois, pas dans une fenetre de 7 jours.

---

### Question 6.1.2

**Requete :**
```javascript
db.transactions.aggregate([
  { $sort: { Customer_ID: 1, Merchant_Category: 1, Transaction_Date: 1 } },
  { $group: {
    _id: { customer: "$Customer_ID", category: "$Merchant_Category" },
    montants: { $push: "$Transaction_Amount" },
    count: { $sum: 1 },
    total: { $sum: "$Transaction_Amount" }
  }},
  { $match: { count: { $gte: 4 }, total: { $gt: 20 } } },
  { $sort: { total: -1 } },
  { $limit: 5 }
])
```

**Resultat :**
```
// Aucun resultat
```

---

### Question 6.2.1

**Requete :**
```javascript
db.transactions.createIndex({ Customer_ID: 1, Fraud_Label: 1, Transaction_Amount: -1, Transaction_Date: 1 })
```

**Avant :**
```javascript
db.transactions.find({
  Customer_ID: 24239,
  Transaction_Date: { $gte: "2025-01-01", $lte: "2025-12-31" },
  Fraud_Label: "Fraud"
}).sort({ Transaction_Amount: -1 }).limit(10).hint({ $natural: 1 }).explain("executionStats")
// executionTimeMillis: 42, totalDocsExamined: 50000, COLLSCAN
```

**Apres :**
```javascript
db.transactions.find({
  Customer_ID: 24239,
  Transaction_Date: { $gte: "2025-01-01", $lte: "2025-12-31" },
  Fraud_Label: "Fraud"
}).sort({ Transaction_Amount: -1 }).limit(10).explain("executionStats")
// executionTimeMillis: 0, totalDocsExamined: 0, totalKeysExamined: 1, IXSCAN
```

42ms avant, 0ms après.

![explain avant optimisation COLLSCAN](screenshots/screenshot_5.png)
![explain COLLSCAN details](screenshots/screenshot_5_bis.png)
![explain apres optimisation IXSCAN](screenshots/screenshot_6.png)
![explain IXSCAN details](screenshots/screenshot_6_bis.png)

---

### Question 6.2.2

**Requete :**
```javascript
db.transactions.findOne({ Customer_ID: 67961, Fraud_Label: "Fraud" }, { _id: 1 })
```

**Resultat :**
```
{ _id: ObjectId('...') }
// executionTimeMillis: 0, totalDocsExamined: 1, totalKeysExamined: 1
```

0ms, bien en dessous des 10ms demandées.

---

### Question 6.3.1

Le dernier mois du dataset c'est mai 2025.

**Requete :**
```javascript
db.createView("public_transactions", "transactions", [
  { $match: { Transaction_Date: { $regex: "^2025-05" } } },
  { $project: { IP_Address: 0, Device_ID: 0, Customer_Home_Location: 0 } }
])
```

**Resultat :**
```
// Vue creee, 413 documents
```

---

### Question 6.3.2

**Requete :**
```javascript
db.createView("fraud_summary_by_merchant_category", "transactions", [
  { $match: { Merchant_Category: { $ne: "" } } },
  { $group: {
    _id: "$Merchant_Category",
    total_transactions: { $sum: 1 },
    total_fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
    montant_total: { $sum: "$Transaction_Amount" },
    montant_moyen: { $avg: "$Transaction_Amount" }
  }},
  { $addFields: {
    taux_fraude: { $round: [{ $multiply: [{ $divide: ["$total_fraudes", "$total_transactions"] }, 100] }, 2] },
    montant_moyen: { $round: ["$montant_moyen", 2] }
  }},
  { $project: { _id: 0, categorie: "$_id", total_transactions: 1, total_fraudes: 1, taux_fraude: 1, montant_total: 1, montant_moyen: 1 } },
  { $sort: { taux_fraude: -1 } }
])
```

**Resultat :**
```
{ categorie: 'Restaurant', total_transactions: 8483, total_fraudes: 427, taux_fraude: 5.03, montant_total: 42496, montant_moyen: 5.01 }
{ categorie: 'ATM', total_transactions: 8401, total_fraudes: 421, taux_fraude: 5.01, montant_total: 42010, montant_moyen: 5 }
{ categorie: 'Fuel', total_transactions: 8358, total_fraudes: 408, taux_fraude: 4.88, montant_total: 41754, montant_moyen: 5 }
{ categorie: 'Grocery', total_transactions: 8252, total_fraudes: 397, taux_fraude: 4.81, montant_total: 41408, montant_moyen: 5.02 }
{ categorie: 'Electronics', total_transactions: 8216, total_fraudes: 389, taux_fraude: 4.73, montant_total: 40798, montant_moyen: 4.97 }
{ categorie: 'Clothing', total_transactions: 8281, total_fraudes: 380, taux_fraude: 4.59, montant_total: 41440, montant_moyen: 5.01 }
```

---

## PARTIE 7 : Rapport d'Analyse et Recommandations

---

### 7.1 Analyse finale

On a 50000 transactions sur janvier-mai 2025 avec 2423 fraudes soit 4.85%. Le dataset est propre avec moins de 0.02% de valeurs manquantes.

Ce qui ressort le plus c'est nouveau marchand + transaction internationale a 6.38% de fraude. Les heures comme 21h a 5.77% ou les villes comme Singapore a 5.09% c'est un peu au dessus de la moyenne mais l'ecart est pas enorme. La distance du domicile toute seule ne discrimine pas, 4.75% a plus de 200km c'est quasi pareil que le global.

Le scoring en 3.4.3 avec 6 criteres ca flag 32804 transactions pour 1814 fraudes c'est beaucoup trop. Et en 5.4.1 les 50 pires scores donnent que 2 vraies fraudes. Du coup mettre des seuils fixes pour tout le monde ca marche pas vraiment, faudrait plutot comparer chaque client a son propre historique.

Cote index le compose Customer_ID + Transaction_Date + Transaction_Amount c'est le plus utile, il couvre les recherches par client et permet des covered queries a 0ms comme en 4.4.1. L'index Fraud_Label fait passer de 48ms a 7ms. Le sparse sur Previous_Fraud_Count sert a rien vu que tous les docs ont le champ, ca fait 330 Ko pour rien.

En prod il faudrait surtout alerter quand un client va chez un nouveau marchand a l'etranger. La collection daily_fraud_stats devrait etre mise a jour avec $merge. Et les grosses agregations comme le top marchands devraient etre pre-calculees au lieu de scanner 50000 docs a chaque fois.

---

### 7.2 Dashboard temps reel

**Fraudes des dernieres 24h :**
```javascript
db.transactions.countDocuments({ Fraud_Label: "Fraud", Transaction_Date: "2025-05-01" })
```

**Categories a risque cette semaine :**
```javascript
db.transactions.aggregate([
  { $match: { Transaction_Date: { $gte: "2025-04-25", $lte: "2025-05-01" } } },
  { $group: { _id: "$Merchant_Category", total: { $sum: 1 }, fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } } } },
  { $addFields: { taux: { $round: [{ $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }, 2] } } },
  { $sort: { taux: -1 } },
  { $limit: 5 }
])
```

**Clients a score critique :**
```javascript
db.transactions.aggregate([
  { $match: { risk_level: "HIGH", Fraud_Label: "Fraud" } },
  { $group: { _id: "$Customer_ID", nb_fraudes: { $sum: 1 }, montant_total: { $sum: "$Transaction_Amount" } } },
  { $sort: { nb_fraudes: -1 } },
  { $limit: 20 }
])
```

**Fraudes aujourd'hui vs hier :**
```javascript
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud", Transaction_Date: { $in: ["2025-05-01", "2025-04-30"] } } },
  { $group: { _id: "$Transaction_Date", montant: { $sum: "$Transaction_Amount" }, nb: { $sum: 1 } } },
  { $sort: { _id: -1 } }
])
```

**Taux de fraude sur 1h :**
```javascript
db.transactions.aggregate([
  { $match: { Transaction_Date: "2025-05-01", Transaction_Time: { $gte: "14:00", $lte: "15:00" } } },
  { $group: { _id: null, total: { $sum: 1 }, fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } } } },
  { $project: { _id: 0, total: 1, fraudes: 1, taux: { $round: [{ $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }, 2] } } }
])
```

---

## BONUS : Taux de valeurs manquantes

**Requete :**
```javascript
var total = db.transactions.countDocuments()
var fields = ["Transaction_ID", "Customer_ID", "Transaction_Amount", "Transaction_Time",
  "Transaction_Date", "Merchant_ID", "Merchant_Category", "Device_ID",
  "Account_Balance", "Daily_Transaction_Count", "Failed_Transaction_Count", "Fraud_Label"]

fields.forEach(function(field) {
  var missing = db.transactions.countDocuments({ $or: [{ [field]: { $exists: false } }, { [field]: null }, { [field]: "" }] })
  if (missing > 0) print(field + ": " + missing + " (" + (missing/total*100).toFixed(2) + "%)")
})
```

**Resultat :**
```
Failed_Transaction_Count: 11 (0.02%)
Customer_ID: 10 (0.02%)
Transaction_Amount: 9 (0.02%)
Transaction_Time: 9 (0.02%)
Merchant_Category: 9 (0.02%)
Device_ID: 9 (0.02%)
Account_Balance: 9 (0.02%)
Daily_Transaction_Count: 9 (0.02%)
Merchant_ID: 7 (0.01%)
Fraud_Label: 4 (0.01%)
Transaction_Date: 3 (0.01%)
Transaction_ID: 3 (0.01%)
```

Tres peu de valeurs manquantes, ce sont des strings vides du CSV. Sur 50000 lignes c'est negligeable.
