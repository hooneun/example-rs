use std::fmt::Debug;
use mongodb::bson::{doc, Document};
use mongodb::{bson, Client, Collection};
use mongodb::options::{ClientOptions, CollectionOptions};
use futures::stream::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

#[tokio::main]
async fn main() {
    let mongodb_uri = "MONGODB_URI";

    let client_option = ClientOptions::parse(mongodb_uri).await.unwrap();
    let client = Client::with_options(client_option).unwrap();

    let db_name = "mydatabase";
    let coll_name = "mycollection";

    // create_collection(&client, db_name, coll_name).await;
    // insert_document(&client, db_name, coll_name).await;
    // get_document(&client, db_name, coll_name).await;
    // delete_document(&client, db_name, coll_name).await;
    // update_document(&client, db_name, coll_name).await;
    // get_documents(&client, db_name, coll_name).await.unwrap();
    // insert_document_generic(&client, db_name, coll_name).await;
    get_document_generic::<User>(&client, db_name, coll_name).await;
}

async fn create_collection(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    db.create_collection(coll_name, None).await.unwrap();
}

async fn insert_document(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    let coll = db.collection(coll_name);

    let doc = doc! {
        "name": "Khanh",
        "age": 28
    };

    coll.insert_one(doc, None).await.unwrap();
}

async fn insert_document_generic(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    let coll = db.collection(coll_name);

    let user = User {
        id: None,
        name: "Hh".to_string(),
        age: 30,
    };

    let res = coll.insert_one(user, None).await.unwrap();
    println!("Inserted document with id {:?}", res.inserted_id);
}

async fn get_document(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    let coll = db.collection::<User>(coll_name);

    let filter = doc! {
        "name": "John",
    };

    let result = coll.find_one(Some(filter), None).await.unwrap();

    match result {
        Some(doc) => println!("Document found: {:?}", doc),
        None => println!("No document found"),
    }
}

async fn get_document_generic<T>(client: &Client, db_name: &str, coll_name: &str)
    where T: Debug + DeserializeOwned + Unpin + Send + Sync {
    let db = client.database(db_name);
    let coll = db.collection::<T>(coll_name);

    let filter = doc! {
        "name": "John",
    };

    let result= coll.find_one(Some(filter), None).await.unwrap();

    match result {
        Some(doc) => println!("Document found: {:?}", doc),
        None => println!("No document found"),
    }
}

async fn get_documents(client: &Client, db_name: &str, coll_name: &str) -> Result<(), mongodb::error::Error> {
    let db = client.database(db_name);
    let coll = db.collection::<Document>(coll_name);

    let mut result = coll.find(None, None).await.unwrap();

    while let Some(doc) = result.try_next().await? {
        println!("Document found: {:?}", doc);
    }

    Ok(())
}

async fn delete_document(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    let coll = db.collection::<Document>(coll_name);

    let filter = doc! {
        "name": "John",
    };

    coll.delete_one(filter, None).await.unwrap();
}

async fn update_document(client: &Client, db_name: &str, coll_name: &str) {
    let db = client.database(db_name);
    let coll = db.collection::<Document>(coll_name);

    let filter = doc! {
        "name": "John",
    };

    let update = doc! {
        "$set": {
            "age": 35
        }
    };

    coll.update_one(filter, update, None).await.unwrap();
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    #[serde(rename = "_id")]
    id: Option<bson::oid::ObjectId>,
    name: String,
    age: i32,
}

impl Into<Document> for User {
    fn into(self) -> Document {
        doc! {
            "name": self.name,
            "age": self.age
        }
    }
}