import argparse
from pymongo import MongoClient, UpdateOne

ENV_CONFIG = {
    "LOCAL": {
        "sourceUri": "mongodb://localhost:27017",
        "destinationUri": "mongodb://localhost:27018"
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
sourceCollection = "MERCHANT_BLIKLAN_ITEM"

destinationDb = "bliklan_compute_engine"
keywordFeedCollection = "KEYWORD_FEED"
campaignProductDataCollection = "CAMPAIGN_PRODUCT_DATA"


def syncProductNameInBatch(sourceUri, destinationUri, batchSize):
    sourceClient = MongoClient(sourceUri)
    destinationClient = MongoClient(destinationUri)

    sourceCollection = sourceClient[sourceDb][sourceCollection]
    keywordFeed = destinationClient[destinationDb][keywordFeedCollection]
    campaignProductData = destinationClient[destinationDb][campaignProductDataCollection]

    merchantBliklanItems = sourceCollection.find(
        {"itemSku": {"$exists": True}, "productName": {"$exists": True}},
        {"itemSku": 1, "productName": 1}
    )

    itemSkuProductNameMap = {}
    total_updated_kw = 0
    total_updated_campaign = 0

    for bliklanItem in merchantBliklanItems:
        itemSku = bliklanItem["itemSku"]
        productName = bliklanItem["productName"]
        itemSkuProductNameMap[itemSku] = productName

        if len(itemSkuProductNameMap) >= batchSize:
            total_updated_kw += updateProductNameInBatch(keywordFeed, itemSkuProductNameMap, "skuId")
            total_updated_campaign += updateProductNameInBatch(campaignProductData, itemSkuProductNameMap, "SKU_ID")
            itemSkuProductNameMap = {}

    if itemSkuProductNameMap:
        total_updated_kw += updateProductNameInBatch(keywordFeed, itemSkuProductNameMap, "skuId")
        total_updated_campaign += updateProductNameInBatch(campaignProductData, itemSkuProductNameMap, "SKU_ID")

    sourceClient.close()
    destinationClient.close()

    print(f"\nDone.\nKeyword Feed updated: {total_updated_kw}\nCampaign Product Data updated: {total_updated_campaign}")

def updateProductNameInBatch(collection, sku_to_name, field_name):
    updates = [
        UpdateOne(
            {field_name: sku},
            {"$set": {"productName": productName}}
        )
        for sku, productName in sku_to_name.items()
    ]
    if not updates:
        return 0
    try:
        result = collection.bulk_write(updates, ordered=False)
    except Exception as e:
        print(f"Bulk write failed for {collection.name}: {e}")
        return 0
    return result.modified_count


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
    print(f"Running sync for environment: {env}")

    syncProductNameInBatch(config["sourceUri"], config["destinationUri"], args.batch_size)