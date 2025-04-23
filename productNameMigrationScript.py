import argparse
from pymongo import MongoClient, UpdateOne

ENV_CONFIG = {
    "LOCAL": {
        "sourceUri": "mongodb://bliklan_campaign_management:bliklan_campaign_management@central-mongo-v60-01.qa2-sg.cld,central-mongo-v60-02.qa2-sg.cld:27017/bliklan_campaign_management",
        "destinationUri": "mongodb://localhost:27017"
    },
    "QA2": {
        "sourceUri": "mongodb://localhost:27017",
        "destinationUri": "mongodb://localhost:27017"
    },
    "PREPROD": {
        "sourceUri": "mongodb://localhost:27017",
        "destinationUri": "mongodb://localhost:27017"
    },
    "PROD": {
        "sourceUri": "mongodb://localhost:27017",
        "destinationUri": "mongodb://localhost:27017"
    }
}

sourceDb = "bliklan_campaign_management"
merchantBliklanItemsCollection = "MERCHANT_BLIKLAN_ITEM"

destinationDb = "bliklan_compute_engine"
keywordFeedCollection = "KEYWORD_FEED"
campaignProductDataCollection = "CAMPAIGN_PRODUCT_DATA"

def get_distinct_skus(collection, sku_field):
    print(f"📤 Fetching distinct SKUs from {collection.name}...")
    return set(collection.distinct(sku_field))

def get_product_names_for_skus(collection, skus, batch_size=1000):
    sku_name_map = {}
    total_skus = len(skus)
    skus_list = list(skus)
    
    for i in range(0, total_skus, batch_size):
        batch_skus = skus_list[i:i + batch_size]
        print(f"📤 Fetching product names for SKUs batch {i+1}-{min(i+batch_size, total_skus)} of {total_skus}")
        
        items = collection.find(
            {
                "itemSku": {"$in": batch_skus},
                "productName": {"$exists": True}
            },
            {"itemSku": 1, "productName": 1}
        )
        
        for item in items:
            if "itemSku" in item and "productName" in item:
                sku_name_map[item["itemSku"]] = item["productName"]
    
    return sku_name_map

def syncProductNameInBatch(sourceUri, destinationUri, batchSize):
    print("🔗 Connecting to databases...")
    try:
        sourceClient = MongoClient(sourceUri, serverSelectionTimeoutMS=10000)
        destinationClient = MongoClient(destinationUri, serverSelectionTimeoutMS=10000)

        sourceCollection = sourceClient[sourceDb][merchantBliklanItemsCollection]
        keywordFeed = destinationClient[destinationDb][keywordFeedCollection]
        campaignProductData = destinationClient[destinationDb][campaignProductDataCollection]

        print("\n📥 Collecting SKUs...")
        kw_skus = get_distinct_skus(keywordFeed, "skuId")
        campaign_skus = get_distinct_skus(campaignProductData, "SKU_ID")
        
        all_needed_skus = kw_skus.union(campaign_skus)
        print(f"   ▸ Total unique SKUs: {len(all_needed_skus)}")

        print("\n📥 Fetching product names...")
        itemSkuProductNameMap = get_product_names_for_skus(sourceCollection, all_needed_skus)
        print(f"   ▸ Found names for {len(itemSkuProductNameMap)} SKUs")

        total_updated_kw = processInBatches(keywordFeed, "skuId", itemSkuProductNameMap, batchSize)
        total_updated_campaign = processInBatches(campaignProductData, "SKU_ID", itemSkuProductNameMap, batchSize)

        sourceClient.close()
        destinationClient.close()

        print("\n✅ Sync Complete")
        print(f"   ▸ KEYWORD_FEED updates: {total_updated_kw}")
        print(f"   ▸ CAMPAIGN_PRODUCT_DATA updates: {total_updated_campaign}")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

def processInBatches(collection, sku_field, sku_to_name_map, batch_size):
    updates = []
    modified_total = 0
    missing_product_name = 0
    matching_product_name = 0
    skus_without_source_data = set()

    print(f"\n📁 Processing collection: {collection.name}")
    
    cursor = collection.find({})
    processed_count = 0
    
    for doc in cursor:
        processed_count += 1
        sku = doc.get(sku_field)
        if not sku:
            continue

        new_name = sku_to_name_map.get(sku)
        if not new_name:
            skus_without_source_data.add(sku)
            continue

        should_update = False
        current_name = doc.get("productName")
        
        if "productName" not in doc:
            missing_product_name += 1
            should_update = True
        elif current_name != new_name:
            should_update = True
        else:
            matching_product_name += 1

        if should_update:
            update_op = UpdateOne(
                {
                    "_id": doc["_id"],
                    sku_field: sku
                },
                {"$set": {"productName": new_name}},
                upsert=False
            )
            updates.append(update_op)

        if len(updates) >= batch_size:
            modified_total += executeBulkUpdate(collection, updates)
            updates.clear()

    if updates:
        modified_total += executeBulkUpdate(collection, updates)

    # Final Report
    print(f"\n📊 {collection.name} Update Report:")
    print(f"   ▸ Total Processed: {processed_count}")
    print(f"   ▸ Updated: {modified_total}")
    print(f"   ▸ Already Correct: {matching_product_name}")
    print(f"   ▸ SKUs Not Found: {len(skus_without_source_data)}")
    if skus_without_source_data:
        print(f"   ▸ Missing SKUs: {', '.join(skus_without_source_data)}")
    
    return modified_total

def executeBulkUpdate(collection, updates):
    try:
        if not updates:
            return 0
            
        print(f"   ▸ Updating batch of {len(updates)} documents...")
        result = collection.bulk_write(updates, ordered=False)
        return result.modified_count
    except Exception as e:
        print(f"❌ Update failed: {str(e)}")
        return 0

def parse_args():
    parser = argparse.ArgumentParser(description="Sync productName across MongoDB environments")
    parser.add_argument(
        "--env", type=str,
        choices=["LOCAL", "QA2", "PREPROD", "PROD"], default="LOCAL",
        help="Environment to run the sync in (default: LOCAL)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=10000,
        help="Batch size for updates"
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    env = args.env.upper()
    config = ENV_CONFIG[env]

    print(f"🚀 Starting sync for environment: {env}")
    syncProductNameInBatch(config["sourceUri"], config["destinationUri"], args.batch_size)