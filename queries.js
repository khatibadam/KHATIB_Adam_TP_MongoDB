// TP MongoDB - FraudShield Banking
// KHATIB Adam - Bac+3 Dev - Isitech Lyon - Mars 2026
// toutes les requetes sont a executer dans mongosh dans l'ordre

// PARTIE 1

// --- Question 1.1.1 ---
use("fraudshield_banking")

// --- Question 1.1.3 ---
// import en bash :
// mongoimport --uri="mongodb://root:example@127.0.0.1:27017/?authSource=admin" --db fraudshield_banking --collection transactions --type csv --headerline --file "FraudShield_Banking_Data.csv"
// 50000 documents importes

db.transactions.countDocuments()
// 50000

db.transactions.findOne()
// les champs numeriques sont en int32, les dates et Yes/No restent en string

// renommage des colonnes qui ont des parentheses dans le nom
db.transactions.updateMany({}, {
  $rename: {
    "Transaction_Amount (in Million)": "Transaction_Amount",
    "Account_Balance (in Million)": "Account_Balance",
    "Avg_Transaction_Amount (in Million)": "Avg_Transaction_Amount",
    "Max_Transaction_Last_24h (in Million)": "Max_Transaction_Last_24h"
  }
})
// matchedCount: 50000, modifiedCount: 50000

// --- Question 1.2.1 ---
db.transactions.find().limit(5)

// --- Question 1.2.2 ---
// conversion des Yes/No en booleens pour 3 champs
db.transactions.updateMany({ Is_International_Transaction: "Yes" }, { $set: { Is_International_Transaction: true } })
db.transactions.updateMany({ Is_International_Transaction: "No" }, { $set: { Is_International_Transaction: false } })

db.transactions.updateMany({ Is_New_Merchant: "Yes" }, { $set: { Is_New_Merchant: true } })
db.transactions.updateMany({ Is_New_Merchant: "No" }, { $set: { Is_New_Merchant: false } })

db.transactions.updateMany({ Unusual_Time_Transaction: "Yes" }, { $set: { Unusual_Time_Transaction: true } })
db.transactions.updateMany({ Unusual_Time_Transaction: "No" }, { $set: { Unusual_Time_Transaction: false } })


// PARTIE 2

// --- Question 2.1.1 ---
db.transactions.countDocuments()
// 50000
db.transactions.countDocuments({ Fraud_Label: "Fraud" })
// 2423
db.transactions.countDocuments({ Fraud_Label: "Normal" })
// 47573
// taux de fraude : 2423/50000 = 4.85%

// --- Question 2.1.2 ---
// le sort desc remonte un doc avec montant vide donc on filtre sur type number
db.transactions.find({ Transaction_Amount: { $type: "number" } }).sort({ Transaction_Amount: -1 }).limit(1)
// Transaction_ID 902451, montant 9 millions, pas frauduleuse

// --- Question 2.1.3 ---
db.transactions.aggregate([
  { $group: { _id: "$Customer_ID", nombre_transactions: { $sum: 1 } } },
  { $sort: { nombre_transactions: -1 } },
  { $limit: 10 },
  { $project: { _id: 0, Customer_ID: "$_id", nombre_transactions: 1 } }
])

// --- Question 2.2.1 ---
db.transactions.countDocuments({
  Transaction_Amount: { $gt: 5 },
  Is_International_Transaction: true,
  Card_Type: "Credit",
  Previous_Fraud_Count: { $gt: 0 }
})
// 2667

db.transactions.countDocuments({
  Transaction_Amount: { $gt: 5 },
  Is_International_Transaction: true,
  Card_Type: "Credit",
  Previous_Fraud_Count: { $gt: 0 },
  Fraud_Label: "Fraud"
})
// 173 soit 6.49%

// --- Question 2.2.2 ---
db.transactions.countDocuments({
  Unusual_Time_Transaction: true,
  Distance_From_Home: { $gt: 100 }
})
// 20734

db.transactions.countDocuments({
  Unusual_Time_Transaction: true,
  Distance_From_Home: { $gt: 100 },
  Fraud_Label: "Fraud"
})
// 1231 soit 5.94%

// --- Question 2.2.3 ---
db.transactions.find(
  { Merchant_Category: { $in: ["Clothing", "Electronics", "Restaurant"] } },
  { Transaction_ID: 1, Transaction_Amount: 1, Merchant_Category: 1, Fraud_Label: 1, _id: 0 }
).limit(10)

// --- Question 2.3.1 ---
// correction des faux positifs du client 24239 le 15 janvier
db.transactions.updateMany(
  { Customer_ID: 24239, Transaction_Date: "2025-01-15", Fraud_Label: "Fraud" },
  { $set: { Fraud_Label: "Normal" } }
)
// matchedCount: 0, aucune transaction a corriger

// --- Question 2.3.2 ---
// tout le monde a LOW d'abord
db.transactions.updateMany({}, { $set: { risk_level: "LOW" } })

// ceux qui matchent passent a MEDIUM
db.transactions.updateMany(
  { $or: [
    { Transaction_Amount: { $gt: 5 } },
    { Is_International_Transaction: true },
    { Failed_Transaction_Count: { $gt: 3 } }
  ]},
  { $set: { risk_level: "MEDIUM" } }
)

// les plus risques passent a HIGH
db.transactions.updateMany(
  { $or: [
    { Transaction_Amount: { $gt: 10 } },
    { Previous_Fraud_Count: { $gt: 2 } },
    { Distance_From_Home: { $gt: 500 } }
  ]},
  { $set: { risk_level: "HIGH" } }
)

// --- Question 2.3.3 ---
// anonymisation RGPD des IP de janvier 2025
db.transactions.updateMany(
  { Transaction_Date: { $gte: "2025-01-01", $lte: "2025-01-31" } },
  { $set: { IP_Address: "ANONYMIZED" } }
)
// 12691 documents modifies

// --- Question 2.4.1 ---
// copie dans archive
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud", Failed_Transaction_Count: { $gte: 2 } } },
  { $out: "archive_transactions" }
])

db.archive_transactions.countDocuments()
// 773

// suppression de la collection principale
db.transactions.deleteMany({ Fraud_Label: "Fraud", Failed_Transaction_Count: { $gte: 2 } })
// deletedCount: 773

db.transactions.countDocuments({ Fraud_Label: "Fraud", Failed_Transaction_Count: { $gte: 2 } })
// 0

// reinsertion pour la suite du TP
db.archive_transactions.find().forEach(function(doc) {
  delete doc._id;
  db.transactions.insertOne(doc);
})


// PARTIE 3

// --- Question 3.1.1 ---
// heures avec le plus de fraudes
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud" } },
  { $project: { heure: { $toInt: { $substr: ["$Transaction_Time", 0, 2] } } } },
  { $group: { _id: "$heure", nombre_fraudes: { $sum: 1 } } },
  { $sort: { nombre_fraudes: -1 } }
])
// 21h en tete avec 118 fraudes

// --- Question 3.1.2 ---
// clients avec 10+ transactions par jour et au moins une fraude
db.transactions.aggregate([
  { $group: {
    _id: { customer: "$Customer_ID", date: "$Transaction_Date" },
    count: { $sum: 1 },
    has_fraud: { $max: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
  }},
  { $match: { count: { $gt: 10 }, has_fraud: 1 } },
  { $project: { _id: 0, Customer_ID: "$_id.customer", nombre_transactions: "$count" } }
])
// aucun resultat, le max par client par jour c'est 2

// --- Question 3.2.1 ---
// top 5 localisations par taux de fraude
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
// Singapore 5.09%, Bangkok 5.07%, London 4.92%, Faisalabad 4.9%, Kuala Lumpur 4.87%

// --- Question 3.2.2 ---
// transactions a distance avec localisation differente du domicile
db.transactions.countDocuments({
  $expr: { $ne: ["$Transaction_Location", "$Customer_Home_Location"] },
  Distance_From_Home: { $gt: 200 }
})
// 30027

db.transactions.countDocuments({
  $expr: { $ne: ["$Transaction_Location", "$Customer_Home_Location"] },
  Distance_From_Home: { $gt: 200 },
  Fraud_Label: "Fraud"
})
// 1426 soit 4.75%

// --- Question 3.3.1 ---
// top 10 marchands par montant total de fraudes
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

// --- Question 3.3.2 ---
// ratio credit/debit par categorie
// j'utilise $cond dans le group pour compter credit et debit en meme temps
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
// ratios entre 0.97 et 1.01, pas de difference

// --- Question 3.4.1 ---
// transactions qui depassent 300% de la moyenne du client
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
// 10093 transactions, 510 fraudes, taux 5.05%

// --- Question 3.4.2 ---
// nouveau marchand ET transaction internationale
db.transactions.countDocuments({ Is_New_Merchant: true, Is_International_Transaction: true })
// 12598

db.transactions.countDocuments({ Is_New_Merchant: true, Is_International_Transaction: true, Fraud_Label: "Fraud" })
// 804 soit 6.38%

// --- Question 3.4.3 ---
// transactions suspectes avec au moins 3 criteres sur 6
// j'ai mis 6 criteres : montant, heure, nouveau marchand, international, distance, nb transactions
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
// 32804 transactions, 1814 fraudes, taux 5.53%


// PARTIE 4

// --- Question 4.1.1 ---
// on a converti les Yes/No en booleens en partie 1 donc true au lieu de "Yes"
db.transactions.find({
  Transaction_Amount: { $gt: 5 },
  Fraud_Label: "Fraud",
  Is_International_Transaction: true
}).explain("executionStats")
// executionTimeMillis: 48, totalDocsExamined: 50000, nReturned: 679, COLLSCAN

// --- Question 4.1.2 ---
// requete 1 : recherche par client
db.transactions.find({ Customer_ID: 24239 }).explain("executionStats")
// 38ms, 50000 docs examines, 1 retourne, COLLSCAN

// requete 2 : fraudes recentes
db.transactions.find({ Fraud_Label: "Fraud", Transaction_Date: { $gte: "2025-03-01" } }).explain("executionStats")
// 42ms, 50000 docs, 1210 retournes, COLLSCAN

// requete 3 : transactions par marchand
db.transactions.find({ Merchant_ID: 97028, Transaction_Amount: { $gt: 5 } }).explain("executionStats")
// 34ms, 50000 docs, 2 retournes, COLLSCAN

// --- Question 4.2.1 ---
db.transactions.createIndex({ Fraud_Label: 1 })
// apres re-execution de 4.1.1 : 7ms, 2423 docs examines, IXSCAN

// --- Question 4.2.2 ---
// egalite puis sort puis range
db.transactions.createIndex({ Customer_ID: 1, Transaction_Date: -1, Transaction_Amount: 1 })

db.transactions.find({
  Customer_ID: 24239,
  Transaction_Amount: { $gte: 1, $lte: 10 }
}).sort({ Transaction_Date: -1 }).explain("executionStats")
// 1ms, 1 doc examine, IXSCAN

// --- Question 4.2.3 ---
db.transactions.createIndex({ Transaction_Location: 1, Merchant_Category: 1 })

db.transactions.find({ Transaction_Location: "Singapore", Merchant_Category: "Electronics" }).explain("executionStats")
// 2ms, 837 docs examines = 837 retournes, IXSCAN

// --- Question 4.2.4 ---
db.transactions.createIndex({ IP_Address: 1 }, { unique: true })
// erreur E11000 : doublons sur IP vides et "ANONYMIZED"
// solution : index unique partiel avec partialFilterExpression: { IP_Address: { $nin: ["", "ANONYMIZED"] } }

// --- Question 4.3.1 ---
// index partiel : indexe que les grosses fraudes
db.transactions.createIndex(
  { Transaction_Amount: 1 },
  { partialFilterExpression: { Fraud_Label: "Fraud", Transaction_Amount: { $gt: 1 } } }
)
// 32 Ko au lieu de 250+ Ko pour un index complet

// --- Question 4.3.2 ---
db.transactions.createIndex({ Previous_Fraud_Count: 1 }, { sparse: true })
// un sparse indexe pas les docs ou le champ existe pas
// ici tous les docs ont le champ donc ca change rien

// --- Question 4.3.3 ---
db.transactions.getIndexes()
db.transactions.stats().indexSizes
// _id_: 524288, Fraud_Label_1: 253952, le compose: 1261568, Location_Category: 339968
// l'index sparse Previous_Fraud_Count fait 331776 pour rien, on peut le supprimer

// --- Question 4.4.1 ---
// l'index compose Customer_ID/Date/Amount couvre deja les champs de la projection
db.transactions.find(
  { Customer_ID: 24239 },
  { Customer_ID: 1, Transaction_Amount: 1, Transaction_Date: 1, _id: 0 }
).explain("executionStats")
// executionTimeMillis: 0, totalDocsExamined: 0, totalKeysExamined: 1, PROJECTION_COVERED


// PARTIE 5

// --- Question 5.1.1 ---
// stats par type de carte
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
// Debit: 125167, Credit: 124769

// --- Question 5.1.2 ---
// taux de fraude par categorie de marchand
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
// aucun resultat, le max c'est Restaurant a 5.03%

// --- Question 5.1.3 ---
// top 20 clients par solde de compte
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
// tous les top 20 sont a 39 millions

// --- Question 5.2.1 ---
// analyse hebdomadaire des fraudes
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

// --- Question 5.2.2 ---
// comportement par historique de fraude
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
// Propres: 4.74%, Risque modere: 4.95%, pas de groupe Haut risque car le max est 2

// --- Question 5.2.3 ---
// top 5 heures les plus risquees
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
// 21h: 5.77%, 13h: 5.48%, 23h: 5.36%, 8h: 5.27%, 14h: 5.27%

// --- Question 5.3.1 ---
// creation de 10 marchands fictifs
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

// jointure transactions frauduleuses avec infos marchands
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud", Merchant_ID: { $in: [96715, 95981, 50678, 19716, 92541, 48263, 85954, 44621, 33744, 43064] } } },
  { $lookup: { from: "merchants", localField: "Merchant_ID", foreignField: "Merchant_ID", as: "merchant_info" } },
  { $unwind: "$merchant_info" },
  { $project: { _id: 0, Transaction_ID: 1, Transaction_Amount: 1, Merchant_ID: 1, "merchant_info.nom": 1, "merchant_info.adresse": 1 } },
  { $limit: 5 }
])

// --- Question 5.3.2 ---
// creation de 10 clients fictifs
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

// profil de risque client avec lookup
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

// --- Question 5.4.1 ---
// score de suspicion avec des poids differents selon le critere
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
// 50 transactions, 2 fraudes, score max 14

// --- Question 5.4.2 ---
// on stocke les stats par jour dans une collection separee
db.transactions.aggregate([
  { $match: { Transaction_Date: { $ne: "" } } },
  { $group: {
    _id: "$Transaction_Date",
    nombre_transactions: { $sum: 1 },
    nombre_fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
    montant_total_fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, "$Transaction_Amount", 0] } }
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
// 121 documents crees


// PARTIE 6

// --- Question 6.1.1 ---
// clients avec au moins 3 fraudes
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
// un seul client avec 3 fraudes mais espacees d'un mois, pas dans une fenetre de 7 jours

// --- Question 6.1.2 ---
// patterns de blanchiment : meme client, meme categorie, 4+ transactions, total > 20M
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
// aucun resultat

// --- Question 6.2.1 ---
db.transactions.createIndex({ Customer_ID: 1, Fraud_Label: 1, Transaction_Amount: -1, Transaction_Date: 1 })

// avant avec hint natural pour forcer le COLLSCAN
db.transactions.find({
  Customer_ID: 24239,
  Transaction_Date: { $gte: "2025-01-01", $lte: "2025-12-31" },
  Fraud_Label: "Fraud"
}).sort({ Transaction_Amount: -1 }).limit(10).hint({ $natural: 1 }).explain("executionStats")
// 42ms, 50000 docs, COLLSCAN

// apres avec l'index
db.transactions.find({
  Customer_ID: 24239,
  Transaction_Date: { $gte: "2025-01-01", $lte: "2025-12-31" },
  Fraud_Label: "Fraud"
}).sort({ Transaction_Amount: -1 }).limit(10).explain("executionStats")
// 0ms, 0 docs examines, IXSCAN

// --- Question 6.2.2 ---
// l'index compose Customer_ID + Fraud_Label est deja en place
db.transactions.findOne({ Customer_ID: 67961, Fraud_Label: "Fraud" }, { _id: 1 })
// 0ms, bien en dessous des 10ms

// --- Question 6.3.1 ---
// vue qui masque les infos sensibles, mois le plus recent = mai 2025
db.createView("public_transactions", "transactions", [
  { $match: { Transaction_Date: { $regex: "^2025-05" } } },
  { $project: { IP_Address: 0, Device_ID: 0, Customer_Home_Location: 0 } }
])

// --- Question 6.3.2 ---
// vue resume par categorie de marchand
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


// PARTIE 7 : Dashboard

// Metrique 1 : fraudes dans les dernieres 24h
db.transactions.countDocuments({ Fraud_Label: "Fraud", Transaction_Date: "2025-05-01" })

// Metrique 2 : top 5 categories a risque cette semaine
db.transactions.aggregate([
  { $match: { Transaction_Date: { $gte: "2025-04-25", $lte: "2025-05-01" } } },
  { $group: { _id: "$Merchant_Category", total: { $sum: 1 }, fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } } } },
  { $addFields: { taux: { $round: [{ $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }, 2] } } },
  { $sort: { taux: -1 } },
  { $limit: 5 }
])

// Metrique 3 : clients avec score critique
db.transactions.aggregate([
  { $match: { risk_level: "HIGH", Fraud_Label: "Fraud" } },
  { $group: { _id: "$Customer_ID", nb_fraudes: { $sum: 1 }, montant_total: { $sum: "$Transaction_Amount" } } },
  { $sort: { nb_fraudes: -1 } },
  { $limit: 20 }
])

// Metrique 4 : montant fraudes aujourd'hui vs hier
db.transactions.aggregate([
  { $match: { Fraud_Label: "Fraud", Transaction_Date: { $in: ["2025-05-01", "2025-04-30"] } } },
  { $group: { _id: "$Transaction_Date", montant: { $sum: "$Transaction_Amount" }, nb: { $sum: 1 } } },
  { $sort: { _id: -1 } }
])

// Metrique 5 : taux de fraude glissant sur 1h
db.transactions.aggregate([
  { $match: { Transaction_Date: "2025-05-01", Transaction_Time: { $gte: "14:00", $lte: "15:00" } } },
  { $group: { _id: null, total: { $sum: 1 }, fraudes: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } } } },
  { $project: { _id: 0, total: 1, fraudes: 1, taux: { $round: [{ $multiply: [{ $divide: ["$fraudes", "$total"] }, 100] }, 2] } } }
])


// BONUS

var total = db.transactions.countDocuments()
var fields = ["Transaction_ID", "Customer_ID", "Transaction_Amount", "Transaction_Time",
  "Transaction_Date", "Merchant_ID", "Merchant_Category", "Device_ID",
  "Account_Balance", "Daily_Transaction_Count", "Failed_Transaction_Count", "Fraud_Label"]

fields.forEach(function(field) {
  var missing = db.transactions.countDocuments({ $or: [{ [field]: { $exists: false } }, { [field]: null }, { [field]: "" }] })
  if (missing > 0) print(field + ": " + missing + " (" + (missing/total*100).toFixed(2) + "%)")
})
// moins de 0.02% de manquants sur tous les champs
