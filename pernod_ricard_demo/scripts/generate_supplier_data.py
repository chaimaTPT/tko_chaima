"""
Generate synthetic supplier data for Pernod Ricard Japan demo.
Bilingual Japanese/English supplier records with realistic inconsistencies.
"""
import os
import random
from databricks.connect import DatabricksSession

CATALOG = "opm_catalog"
SCHEMA = "supplier_hub"

def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksEnv
        env = (DatabricksEnv()
               .withDependencies("dbldatagen==0.4.0.post1")
               .withDependencies("jmespath==1.0.1")
               .withDependencies("pyparsing==3.2.5"))
        return (DatabricksSession.builder
                .serverless(True)
                .withEnvironment(env)
                .profile("fe-vm-serverless-opm")
                .getOrCreate())


def generate_raw_suppliers(spark):
    """Generate 500 raw supplier records with realistic inconsistencies."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
    from pyspark.sql import Row
    from datetime import date, timedelta

    prefectures = [
        ("Tokyo", "東京都"), ("Osaka", "大阪府"), ("Aichi", "愛知県"),
        ("Fukuoka", "福岡県"), ("Hokkaido", "北海道"), ("Kyoto", "京都府"),
        ("Hyogo", "兵庫県"), ("Kanagawa", "神奈川県"), ("Saitama", "埼玉県"),
        ("Chiba", "千葉県"), ("Hiroshima", "広島県"), ("Miyagi", "宮城県"),
        ("Niigata", "新潟県"), ("Shizuoka", "静岡県"), ("Nagano", "長野県"),
    ]

    cities_jp = {
        "東京都": ["千代田区", "港区", "新宿区", "渋谷区", "中央区", "品川区"],
        "大阪府": ["大阪市北区", "大阪市中央区", "堺市", "豊中市", "吹田市"],
        "愛知県": ["名古屋市中村区", "名古屋市中区", "豊田市", "岡崎市"],
        "福岡県": ["福岡市博多区", "北九州市", "久留米市"],
        "北海道": ["札幌市中央区", "旭川市", "函館市"],
        "京都府": ["京都市中京区", "京都市下京区", "宇治市"],
        "兵庫県": ["神戸市中央区", "姫路市", "西宮市", "尼崎市"],
        "神奈川県": ["横浜市西区", "川崎市", "相模原市"],
        "埼玉県": ["さいたま市大宮区", "川越市", "所沢市"],
        "千葉県": ["千葉市中央区", "船橋市", "柏市"],
        "広島県": ["広島市中区", "福山市", "呉市"],
        "宮城県": ["仙台市青葉区", "石巻市"],
        "新潟県": ["新潟市中央区", "長岡市"],
        "静岡県": ["静岡市葵区", "浜松市", "沼津市"],
        "長野県": ["長野市", "松本市", "上田市"],
    }

    supplier_names_jp = [
        ("山田ガラス工業株式会社", "Yamada Glass Industries Co., Ltd."),
        ("東洋パッケージング株式会社", "Toyo Packaging Co., Ltd."),
        ("日本醸造資材株式会社", "Nihon Brewing Materials Co., Ltd."),
        ("太平洋物流サービス", "Pacific Logistics Services"),
        ("富士ラベル印刷", "Fuji Label Printing"),
        ("大和コルク製造所", "Yamato Cork Manufacturing"),
        ("三協アルミキャップ", "Sankyo Aluminum Caps"),
        ("北陸原料商事", "Hokuriku Raw Materials Trading"),
        ("関西醸造機器", "Kansai Brewing Equipment"),
        ("中部包装材料", "Chubu Packaging Materials"),
        ("九州物流センター", "Kyushu Distribution Center"),
        ("東北ガラスびん", "Tohoku Glass Bottles"),
        ("信州果実加工", "Shinshu Fruit Processing"),
        ("瀬戸内海運", "Setouchi Maritime Transport"),
        ("四国紙器製作所", "Shikoku Paper Container Works"),
        ("北海道麦芽製造", "Hokkaido Malt Manufacturing"),
        ("札幌フィルター工業", "Sapporo Filter Industries"),
        ("名古屋段ボール", "Nagoya Cardboard Co."),
        ("横浜港湾運輸", "Yokohama Port Transport"),
        ("神戸品質検査サービス", "Kobe Quality Inspection Services"),
        ("広島樽製作所", "Hiroshima Barrel Works"),
        ("新潟米穀商事", "Niigata Rice Trading"),
        ("長野水源管理", "Nagano Water Source Management"),
        ("京都伝統工芸ラベル", "Kyoto Traditional Craft Labels"),
        ("沖縄サトウキビ原料", "Okinawa Sugarcane Materials"),
        ("仙台冷蔵倉庫", "Sendai Cold Storage Warehouse"),
        ("福岡酒類容器", "Fukuoka Liquor Containers"),
        ("静岡緑茶エキス", "Shizuoka Green Tea Extract"),
        ("岡山果汁プラント", "Okayama Juice Plant"),
        ("熊本水質分析", "Kumamoto Water Quality Analysis"),
    ]

    categories_variations = [
        ["ガラスびん", "Glass Bottles", "ガラス瓶", "glass bottle", "ボトル"],
        ["段ボール", "Cardboard", "corrugated box", "包装箱", "梱包材"],
        ["ラベル・印刷", "Labels", "label printing", "ラベル", "印刷"],
        ["コルク・キャップ", "Corks & Caps", "cork", "キャップ", "栓"],
        ["原料・麦芽", "Raw Materials", "malt", "麦芽", "原材料"],
        ["物流・運送", "Logistics", "transport", "運輸", "配送"],
        ["品質検査", "Quality Inspection", "QC", "検査", "品質管理"],
        ["醸造機器", "Brewing Equipment", "equipment", "機器", "設備"],
        ["果実加工", "Fruit Processing", "juice", "果汁", "加工"],
        ["水質管理", "Water Management", "water", "水源", "浄水"],
    ]

    certifications = [
        "ISO 9001", "ISO 22000", "FSSC 22000", "ISO 14001",
        "HACCP", "JAS", "JIS", "有機JAS", None, None, None
    ]

    records = []
    base_date = date(2024, 1, 1)

    for i in range(500):
        name_pair = random.choice(supplier_names_jp)
        use_japanese = random.random() < 0.6
        supplier_name = name_pair[0] if use_japanese else name_pair[1]

        # Add typos/variations sometimes
        if random.random() < 0.15:
            supplier_name = supplier_name.replace("株式会社", "(株)")
        if random.random() < 0.1:
            supplier_name = supplier_name.replace("Co., Ltd.", "Co. Ltd")
        if random.random() < 0.08:
            supplier_name = supplier_name.upper()

        pref = random.choice(prefectures)
        pref_name = pref[1] if use_japanese else pref[0]
        city_list = cities_jp.get(pref[1], ["市内"])
        city = random.choice(city_list)

        # Create messy addresses
        postal = f"{random.randint(100,999)}-{random.randint(1000,9999)}"
        if use_japanese:
            if random.random() < 0.3:
                address = f"〒{postal} {pref_name}{city}{random.randint(1,30)}-{random.randint(1,20)}"
            elif random.random() < 0.5:
                address = f"{pref_name} {city} {random.randint(1,30)}丁目{random.randint(1,20)}番地"
            else:
                address = f"{postal} {pref_name}{city}{random.randint(1,30)}-{random.randint(1,20)}-{random.randint(1,10)}"
        else:
            address = f"{random.randint(1,30)}-{random.randint(1,20)} {city}, {pref_name} {postal}, Japan"

        cat_group = random.choice(categories_variations)
        category = random.choice(cat_group)

        cert = random.choice(certifications)

        unit_price = round(random.uniform(50, 50000), 2)
        # Introduce some anomalies
        if random.random() < 0.05:
            unit_price = round(unit_price * random.uniform(5, 20), 2)

        lead_time = random.randint(3, 90)
        if random.random() < 0.05:
            lead_time = random.randint(200, 500)

        currency = random.choice(["JPY", "JPY", "JPY", "JPY", "USD", "CNY", "円", "yen"])

        reliability = round(random.uniform(0.5, 1.0), 2) if random.random() > 0.1 else None
        last_audit = (base_date + timedelta(days=random.randint(-365, 0))).isoformat() if random.random() > 0.15 else None

        phone = None
        if random.random() > 0.2:
            area = random.choice(["03", "06", "052", "092", "011", "075", "078", "045"])
            phone = f"{area}-{random.randint(1000,9999)}-{random.randint(1000,9999)}"

        contact = None
        if random.random() > 0.2:
            first_names = ["太郎", "花子", "健太", "美咲", "大輔", "由美", "翔太", "愛"]
            last_names = ["田中", "鈴木", "佐藤", "高橋", "渡辺", "伊藤", "山本", "中村"]
            contact = f"{random.choice(last_names)} {random.choice(first_names)}"

        records.append(Row(
            supplier_id=f"SUP-{i+1:04d}",
            supplier_name=supplier_name,
            category=category,
            address=address,
            prefecture=pref_name if random.random() > 0.3 else None,
            phone=phone,
            contact_person=contact,
            certification=cert,
            unit_price=unit_price,
            currency=currency,
            lead_time_days=lead_time,
            reliability_score=reliability,
            last_audit_date=last_audit,
            source_system=random.choice(["SAP", "Excel", "手入力", "EDI", "FAX", "メール"]),
            ingestion_date=date.today().isoformat(),
        ))

    schema = StructType([
        StructField("supplier_id", StringType()),
        StructField("supplier_name", StringType()),
        StructField("category", StringType()),
        StructField("address", StringType()),
        StructField("prefecture", StringType()),
        StructField("phone", StringType()),
        StructField("contact_person", StringType()),
        StructField("certification", StringType()),
        StructField("unit_price", DoubleType()),
        StructField("currency", StringType()),
        StructField("lead_time_days", IntegerType()),
        StructField("reliability_score", DoubleType()),
        StructField("last_audit_date", StringType()),
        StructField("source_system", StringType()),
        StructField("ingestion_date", StringType()),
    ])

    df = spark.createDataFrame(records, schema)
    table_name = f"{CATALOG}.{SCHEMA}.raw_suppliers"
    df.write.mode("overwrite").saveAsTable(table_name)
    count = spark.table(table_name).count()
    print(f"Wrote {count} rows to {table_name}")
    return df


def generate_product_master(spark):
    """Generate product master data."""
    from pyspark.sql import Row

    products = [
        Row(product_id="PRD-001", product_name="Absolut Vodka 700ml", product_name_jp="アブソルート ウォッカ 700ml", category="Spirits", subcategory="Vodka", bottle_type="Glass", volume_ml=700),
        Row(product_id="PRD-002", product_name="Chivas Regal 12Y 750ml", product_name_jp="シーバスリーガル 12年 750ml", category="Spirits", subcategory="Whisky", bottle_type="Glass", volume_ml=750),
        Row(product_id="PRD-003", product_name="Martell VS 700ml", product_name_jp="マーテル VS 700ml", category="Spirits", subcategory="Cognac", bottle_type="Glass", volume_ml=700),
        Row(product_id="PRD-004", product_name="Beefeater Gin 750ml", product_name_jp="ビーフィーター ジン 750ml", category="Spirits", subcategory="Gin", bottle_type="Glass", volume_ml=750),
        Row(product_id="PRD-005", product_name="Malibu 700ml", product_name_jp="マリブ 700ml", category="Spirits", subcategory="Liqueur", bottle_type="Glass", volume_ml=700),
        Row(product_id="PRD-006", product_name="Perrier-Jouet Grand Brut", product_name_jp="ペリエ ジュエ グランブリュット", category="Wine", subcategory="Champagne", bottle_type="Glass", volume_ml=750),
        Row(product_id="PRD-007", product_name="Jacob's Creek Shiraz 750ml", product_name_jp="ジェイコブス クリーク シラーズ 750ml", category="Wine", subcategory="Red Wine", bottle_type="Glass", volume_ml=750),
        Row(product_id="PRD-008", product_name="Kahlua Coffee Liqueur 700ml", product_name_jp="カルーア コーヒーリキュール 700ml", category="Spirits", subcategory="Liqueur", bottle_type="Glass", volume_ml=700),
    ]

    df = spark.createDataFrame(products)
    table_name = f"{CATALOG}.{SCHEMA}.product_master"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    count = spark.table(table_name).count()
    print(f"Wrote {count} rows to {table_name}")


def generate_deliveries(spark):
    """Generate delivery/order history data."""
    from pyspark.sql import Row
    from datetime import date, timedelta

    records = []
    base_date = date(2024, 1, 1)

    for i in range(2000):
        supplier_id = f"SUP-{random.randint(1,500):04d}"
        product_id = f"PRD-{random.randint(1,8):03d}"
        order_date = base_date + timedelta(days=random.randint(0, 365))
        promised_days = random.randint(5, 45)
        actual_days = promised_days + random.randint(-3, 15)
        qty = random.randint(50, 10000)
        on_time = actual_days <= promised_days

        records.append(Row(
            delivery_id=f"DEL-{i+1:05d}",
            supplier_id=supplier_id,
            product_id=product_id,
            order_date=order_date.isoformat(),
            promised_lead_days=promised_days,
            actual_lead_days=max(1, actual_days),
            quantity_ordered=qty,
            quantity_delivered=qty if random.random() > 0.1 else int(qty * random.uniform(0.7, 0.99)),
            on_time_delivery=on_time,
            quality_pass=random.random() > 0.05,
            unit_price_jpy=round(random.uniform(100, 30000), 2),
        ))

    df = spark.createDataFrame(records)
    table_name = f"{CATALOG}.{SCHEMA}.deliveries"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    count = spark.table(table_name).count()
    print(f"Wrote {count} rows to {table_name}")


if __name__ == "__main__":
    print("Initializing Spark session...")
    spark = get_spark()
    print(f"Spark version: {spark.version}")

    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Pernod Ricard supplier data harmonization'")

    print("\n--- Generating raw suppliers (500 records) ---")
    generate_raw_suppliers(spark)

    print("\n--- Generating product master ---")
    generate_product_master(spark)

    print("\n--- Generating delivery history (2000 records) ---")
    generate_deliveries(spark)

    print("\nData generation complete!")
