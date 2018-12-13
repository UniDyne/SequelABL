{constants.i}
{Sequel.i &NEW=NEW &LOG=1}

/*
	Define a temp table that matches
	the schema of the destination table
	in SQL Server
*/
DEFINE TEMP-TABLE ttProduct NO-UNDO
	FIELD sku AS CHAR
    FIELD name AS CHAR
    FIELD upc AS CHAR
    FIELD isbn AS CHAR
    FIELD mpn AS CHAR
    FIELD primary_supplier AS CHAR
    FIELD active AS LOG
    FIELD cansell AS LOG
    FIELD discontinued AS LOG
    FIELD disc_date AS DATE
    FIELD nobackorder AS LOG
    FIELD candropship AS LOG
    FIELD mustdropship AS LOG
	FIELD composite AS LOG
	.


FOR EACH product WHERE product.SYSTEM-ID = {&sysid} AND product.active = YES NO-LOCK,
	FIRST oe-prod-location WHERE oe-prod-location.system-id = {&sysid} AND oe-prod-location.location-key = {&lockey} AND oe-prod-location.product-key = product.product-key NO-LOCK:

	CREATE ttProduct.
	ASSIGN
		ttProduct.sku = TRIM(product.product-code)
        ttProduct.upc = TRIM(TRIM(product.upc-code," "))
        ttProduct.isbn = REPLACE(product.usr-def-fld-9,"-","")

		ttProduct.name = TRIM(product.product-name)

		ttProduct.active = product.active
        ttProduct.discontinued = product.discontinued
        ttProduct.disc_date = product.discontinued-date
        ttProduct.cansell = oe-prod-location.can-be-sold
        ttProduct.composite = oe-prod-location.buying-path-build
        ttProduct.candropship = oe-prod-location.drop-ship-allowed
        ttProduct.mustdropship = oe-prod-location.must-drop-ship
		.

END.

/* Connect to live SQL environment */
RUN ConnectLive IN Sequel.

/*
	Merge this temp table into SQL, matching the 'sku' field.
	Final 'yes' param will delete products not present in temp
	from the SQL table. Set to 'no' to prevent deletion.
*/
RUN MergeTable IN Sequel (INPUT "apprise_product", INPUT "sku", INPUT TEMP-TABLE ttProduct:HANDLE, INPUT YES).

/* Disconnect from SQL */
RUN CloseConnect IN Sequel.

