/*
===============================================================================================================

Stored Procedure: Loading bronze layer (source -> layer)

===============================================================================================================

Purpose:
	This procedure loads the data from external csv files into the tables present in the bronze layer.
	it performs the following actions:
	- truncates the table before loading the data to prevent loading it more than once.
	- insert the values in the table using the 'BULK' command.
	

usage example:
	Exec bronze.load_bronze;

===============================================================================================================
*/

create or alter procedure bronze.load_bronze as
begin
	begin try
		declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;

		set @batch_start_time = GETDATE();
		print '============================================================================';
		print 'loading bronze layer';
		print '============================================================================';

		print '----------------------------------------------------------------------------';
		print 'Loading CRM Tables';
		print '----------------------------------------------------------------------------';
	
		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_cust_info';

		truncate table bronze.crm_cust_info;
		print '>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'c:\scratch\sql-da\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.crm_cust_info;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '-------------------';


		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_prd_info';

		truncate table bronze.crm_prd_info;
		print '>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'c:\scratch\sql-da\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.crm_prd_info;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '-------------------';

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_sales_details';

		truncate table bronze.crm_sales_details;
		print '>> Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'c:\scratch\sql-da\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.crm_sales_details;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '-------------------';

		print '----------------------------------------------------------------------------';
		print 'Loading ERP Tables';
		print '----------------------------------------------------------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: bronze.erp_cust_az12';

		truncate table bronze.erp_cust_az12;
		print '>> Inserting data into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'c:\scratch\sql-da\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.erp_cust_az12;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '-------------------';

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_loc_a101';

		truncate table bronze.erp_loc_a101;
		print 'Inserting Data: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'c:\scratch\sql-da\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.erp_loc_a101;
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '-------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: bronze.erp_px_catg1v2';

		truncate table bronze.erp_px_catg1v2;
		print '>> Inesrting table: bronze.erp_px_catg1v2';
		bulk insert bronze.erp_px_catg1v2
		from 'c:\scratch\sql-da\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.erp_px_catg1v2
		set @end_time = GETDATE();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '-------------------';
		set @batch_end_time = GETDATE();

		print '============================================================================';
		print 'Bronze layer loading is completed';
		print 'Total load duration : ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
		print '============================================================================';

	end try
	begin catch
		print '============================================================================';
		print 'Error while loading the bronze layer';
		print 'Error Message ' + Error_Message();
		print 'Error Message ' + cast(Error_Number() as nvarchar);
		print 'Error Message ' + cast(Error_State() as nvarchar);
		print '============================================================================';
	end catch
end
