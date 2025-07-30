databricks_glossary = {
    "access control list (ACL)": 
        "A list of permissions attached to the workspace, cluster, job, table, or experiment. "
        "An ACL specifies which users or system processes are granted access to the objects, and what operations are allowed on the assets. "
        "Each entry in a typical ACL specifies a subject and an operation. See Access control lists.",

    "access mode": 
        "A security feature that determines who can use a compute resource and the data they can access while using the compute resource. "
        "Every compute resource in Databricks has an access mode. See Access modes.",

    "ACID transactions":
        "Database transactions that are processed reliably. ACID stands for atomicity, consistency, isolation, durability. See Best practices for reliability.",

    "artificial intelligence (AI)":
        "The capability of a computer to imitate intelligent human behavior. See AI and machine learning on Databricks.",

    "AI agent":
        "An application with complex reasoning capabilities that allows it to create its own plan and execute the task according to the tools at its disposal. "
        "See What are compound AI systems and AI agents?.",

    "AI functions":
        "The built-in SQL functions that allow you to apply AI on your data directly from SQL in Databricks. See AI Functions on Databricks.",

    "AI playground":
        "A Databricks feature where users can interact with, test, and compare generative AI models served in your Databricks workspace. "
        "See Chat with LLMs and prototype GenAI apps using AI Playground.",

    "anomaly detection":
        "Techniques and tools used to identify unusual patterns that do not conform to expected behavior in datasets. "
        "Databricks facilitates anomaly detection through its machine learning and data processing capabilities.",

    "Apache Spark":
        "An open-source, distributed computing system used for big data workloads. See Apache Spark on Databricks.",

    "artificial neural network (ANN)":
        "A computing system patterned after the operation of neurons in the human brain.",

    "asset":
        "An entity in a Databricks workspace (for example, an object or a file).",

    "audit log":
        "A record of user activities and actions within the Databricks environment, crucial for security, compliance, and operational monitoring. "
        "See Audit log reference.",

    "Auto Loader":
        "A data ingestion feature that incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup. "
        "See What is Auto Loader?.",

    "AutoML":
        "A Databricks feature that simplifies the process of applying machine learning to your datasets by automatically finding the best algorithm and hyperparameter configuration for you. "
        "See What is AutoML?.",

    "automated data lineage":
        "The process of automatically tracking and visualizing the flow of data from its origin through various transformations to its final form, "
        "essential for debugging, compliance, and understanding data dependencies. Databricks facilitates this through integrations with data lineage tools.",

    "autoscaling, horizontal":
        "Adding or removing executors based on the number of tasks waiting to be scheduled. This happens dynamically during a single update.",

    "autoscaling, vertical":
        "Increasing or decreasing the size of a machine (driver or executor) based on memory pressure (or lack thereof). "
        "This happens only at the start of a new update.",

    "Azure Databricks":
        "A version of Databricks that is optimized for the Microsoft Azure cloud platform.",

    "batch processing":
        "A data processing method that allows you to define explicit instructions to process a fixed amount of static, non-changing data as a single operation. "
        "Databricks uses Spark SQL or DataFrames. See Streaming and incremental ingestion.",

    "bias detection and mitigation":
        "The process of identifying and addressing biases in data and machine learning models to ensure fairness and accuracy. "
        "Databricks offers tools and integrations to help detect and mitigate bias. See Monitor fairness and bias for classification models.",

    "business intelligence (BI)":
        "The strategies and technologies used by enterprises for the data analysis and management of business information.",

    "Catalog Explorer":
        "A Databricks feature that provides a UI to explore and manage data, schemas (databases), tables, models, functions, and other AI assets. "
        "You can use it to find data objects and owners, understand data relationships across tables, and manage permissions and sharing. "
        "See What is Catalog Explorer?.",

    "CICD or CI/CD":
        "The combined practices of continuous integration (CI) and continuous delivery (CD). See What is CI/CD on Databricks?.",

    "clean data":
        "Data that has gone through a data cleansing process, which is the process of detecting and correcting (or removing) corrupt or inaccurate records from a record set, table, or database and refers to identifying incomplete, incorrect, inaccurate, or irrelevant parts of the data and then replacing, modifying, or deleting the dirty or coarse data."
}
