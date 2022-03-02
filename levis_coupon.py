"""
@author: Dibyesh Mishra
@date: 27-02-2022 23:18
"""
import re

import pymysql
pymysql.install_as_MySQLdb()


class Functionality:
    """
    class Functionality have methods which shows data from table
    in database programmigration
    """
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='root',
                                 db='programmigration')
    cursor = connection.cursor()

    def total_rows_with_blank_mobile_no(self, table_name):
        """
        desc: counting the total number of blank mobile numbers
        return: count
        """
        query = "select mobile from %s " % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        i = 0
        count = 0
        while i < len(result):
            if not result[i][0]:
                count = count+1
            i = i+1
        print(count)
        self.connection.commit()
        return count

    def total_rows_with_invalid_mobile_no(self, table_name):
            """
            desc: counting the total number of invalid mobile numbers and inserting those into coupon_error table
            return: count
            """
            query = "select mobile from %s " % table_name
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            i = 0
            count = 0
            while i < len(result):
                r = re.match('^[6-9]{1}[0-9]{9}', result[i][0])
                if not r:
                    count = count + 1
                    query2 ="INSERT INTO couponError(Mobile,BillNo, IssuedDate, UsedDate ,ExpiryDate , IssuedStore ," \
                            " Amount , Discount , Tier  ,  CouponOfferCode,  CouponCode , segement , ClaimedUserName," \
                            " AvailableAmount , UsedAmount , Denomination , EasyCodeDetailStatus , IssueTypeName," \
                            "  RedeemedStore , AccountTypeName , IssueToMobile , IssuedStoreCode,  RedeemedStoreCode," \
                            "  TransactionID , Narration , datecreated  ,lastupdated) SELECT * FROM levis_coupon" \
                            " WHERE Mobile = %s;  " % result[i][0]
                    self.cursor.execute(query2)
                    self.connection.commit()
                    query3 = "UPDATE coupon_error SET reason ='Mobile number is Invalid' WHERE Mobile = %s" % result[i][0]
                    self.cursor.execute(query3)
                    self.connection.commit()
                i = i + 1
            print(count)
            return count

    def total_count_if_amount_is_null(self,table_name):
        query = "SELECT COUNT(*)  FROM %s WHERE Amount = 0 " % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(result[0][0])
        return result[0][0]

    def total_distinct_store(self,table_name):
        query = "SELECT COUNT(DISTINCT issuedStore ) FROM %s" % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(result[0][0])
        return result[0][0]


    def total_duplicate_bill_no(self,table_name):
        query = "SELECT COUNT(*) AS Total_duplicate_count FROM (SELECT BillNo FROM %s GROUP BY BillNo HAVING " \
                "COUNT(BillNo) > 1) AS X" % table_name
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(result[0][0])
        return result[0][0]

    def insert_into_dqr(self,table_name):
        """
        desc: inserting the values into the tier_dqr table
        """
        total_rows_with_blank_mobile = self.total_rows_with_blank_mobile_no("levis_coupon")
        total_rows_with_invalid_mobile = self.total_rows_with_invalid_mobile_no("levis_coupon")
        total_count_if_amount_is_null = self.total_count_if_amount_is_null("levis_coupon")
        total_distinct_store = self.total_distinct_store("levis_coupon")
        total_duplicate_bill_no = self.total_duplicate_bill_no("levis_coupon")

        query = "insert into %s(Total_Invalid_Mobile,Total_Blank_Mobile," \
                 "Amount_is_null,distinct_stores,Total_duplicate_BillNo) values(%s,%s,%s,%s,%s)"\
                                                                            % (table_name,total_rows_with_blank_mobile,
                                                                                total_rows_with_invalid_mobile,
                                                                                total_count_if_amount_is_null,
                                                                                total_distinct_store,
                                                                                total_duplicate_bill_no)
        self.cursor.execute(query)
        self.connection.commit()



data = Functionality()
# data.total_rows_with_blank_mobile_no("levis_coupon")
# data.total_rows_with_invalid_mobile_no("levis_coupon")
# data.total_count_if_amount_is_null("levis_coupon")
# data.total_distinct_store("levis_coupon")
# data.total_duplicate_bill_no("levis_coupon")
data.insert_into_dqr("coupondqr")