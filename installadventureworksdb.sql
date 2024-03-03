CREATE TABLE [SalesOrderDetail] (
  [SalesOrderID] INTEGER NOT NULL,
  [SalesOrderDetailID] INTEGER IDENTITY (1, 1) NOT NULL,
  [CarrierTrackingNumber] TEXT,
  [OrderQty] INTEGER NOT NULL,
  [ProductID] INTEGER NOT NULL,
  [SpecialOfferId] INTEGER NOT NULL,
  [UnitPrice] INTEGER NOT NULL,
  [UnitPriceDiscount] INTEGER NOT NULL,
  [LineTotal] INTEGER NOT NULL,
  [rowguid] TEXT UNIQUE NOT NULL,
  [ModifiedDate] DATETIME NOT NULL,
  PRIMARY KEY ([SalesOrderID], [SalesOrderDetailID])
);

.separator ,
.import SalesOrderDetail.csv SalesOrderDetail

VACUUM;
