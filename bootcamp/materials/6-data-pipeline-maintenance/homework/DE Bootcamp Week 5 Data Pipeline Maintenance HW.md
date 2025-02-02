# Week 5 Data Pipeline Maintenance HW

## **Pipeline Ownership**

### **Profit Pipelines**

1. **Unit-level profit needed for experiments**  
   * **Primary Owner:** Alex  
   * **Secondary Owner:** Jamie  
2. **Aggregate profit reported to investors**  
   * **Primary Owner:** Jamie  
   * **Secondary Owner:** Sam

### **Growth Pipelines**

3. **Aggregate growth reported to investors**  
   * **Primary Owner:** Sam  
   * **Secondary Owner:** Taylor  
4. **Daily growth needed for experiments**  
   * **Primary Owner:** Taylor  
   * **Secondary Owner:** Alex

### **Engagement Pipelines**

5. **Aggregate engagement reported to investors**  
   * **Primary Owner:** Alex  
   * **Secondary Owner:** Sam

## 

## **On-Call Schedule**

To ensure fairness and account for holidays, the on-call schedule rotates weekly among the four data engineers.

### **Rotation Schedule:**

* **Week 1:** Alex (Primary), Jamie (Secondary)  
* **Week 2:** Jamie (Primary), Sam (Secondary)  
* **Week 3:** Sam (Primary), Taylor (Secondary)  
* **Week 4:** Taylor (Primary), Alex (Secondary)

### **Holiday Coverage:**

* During major holidays, responsibilities are pre-assigned to ensure balanced workloads.  
* Engineers will swap weeks when personal holidays arise, ensuring consistent coverage.

## 

## **Run Books for Investor Reporting Pipelines**

### **Profit Pipeline: Aggregate Profit Reported to Investors**

**Primary Owner:** Jamie  
**Secondary Owner:** Sam

**Common Issues:**

* **Data Anomalies:** Sudden spikes or drops in profit metrics due to incorrect data ingestion from sales reports.  
* **Delayed Data Loads:** Profit data from external sources may not arrive on time, affecting daily reports.  
* **Currency Conversion Errors:** Incorrect exchange rates applied, causing inaccuracies in aggregated profit.

**Critical Downstream Owners:**

* Financial Reporting Team  
* Investor Relations Team

**SLAs and Agreements:**

* Data should be available by 4 hours after UTC midnight for accurate investor reporting.

### **Growth Pipeline: Aggregate Growth Reported to Investors**

**Primary Owner:** Sam  
**Secondary Owner:** Taylor

**Common Issues:**

* **Missing Data:** User signup or activity data may be incomplete due to tracking failures.  
* **Inconsistent Metrics:** Differences in growth metrics between dashboards and raw data due to outdated transformation scripts.  
* **Timezone Misalignment:** Growth metrics aggregated in the wrong timezone, leading to misreported daily growth.

**Critical Downstream Owners:**

* Growth Strategy Team  
* Investor Relations Team

**SLAs and Agreements:**

* Growth metrics must be updated and verified by 5 AM UTC for investor reporting.

**Engagement Pipeline: Aggregate Engagement Reported to Investors**

**Primary Owner:** Alex  
**Secondary Owner:** Sam

**Common Issues:**

* **Event Tracking Failures:** Incomplete engagement data due to errors in the event tracking system.  
* **Duplicate Events:** Multiple counts of the same user event, inflating engagement metrics.  
* **Data Lag:** Real-time data ingestion delays, affecting the freshness of engagement reports.

**Critical Downstream Owners:**

* Product Management Team  
* Investor Relations Team

**SLAs and Agreements:**

* Engagement data should be fully processed and available by 6 AM UTC for investor communications.

**Potential Issues Across All Pipelines**

1. **Data Quality Issues:**  
   * Null or missing values in critical fields (e.g., profit amounts, user activity).  
   * Inconsistent data formats or schema mismatches.  
2. **ETL Failures:**  
   * Broken transformation scripts due to code changes or outdated dependencies.  
   * Incomplete data loads from upstream sources.  
3. **Infrastructure Failures:**  
   * Database connection issues.  
   * Data storage limits reached, causing write failures.  
4. **Latency and Delays:**  
   * Slow data ingestion due to high traffic volumes.  
   * Delays in downstream reporting tools receiving updated data.  
5. **Version Control Issues:**  
   * Incorrect deployment of pipeline updates, leading to data discrepancies.  
   * Lack of rollback mechanisms for failed updates.