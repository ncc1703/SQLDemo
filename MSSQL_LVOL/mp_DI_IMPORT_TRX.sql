--$Workfile: $ 
--$Revision: 124282 $ 
--$Modtime: $ 
--$Author: dge $ 

--	This script is the main process to run Transaction Import processing

if (select count(*) from sysobjects where  name = 'mp_DI_IMPORT_TRX' and type = 'P') > 0
	drop procedure dbo.mp_DI_IMPORT_TRX
go

create procedure	dbo.mp_DI_IMPORT_TRX	@event_id			int,		--expect to have Event log created from outside
										@itn_size			int,		--size of one pass iteration (in investment positions)
										@user_sysid			int,		--id of user running processing
										@commit_flag		int = 1		--default is committing run, that is how main run processing expects it to behave
as
begin

set nocount on

declare @stc_event_id	int,
		@process_cd		int,
		@rec_count		int,				--track how many transaction records have been processed
		@trx_total		int,				--total number of transactions to process
		@ret			int,
		@holding_count	int,				--track how many holdings we have for processing
		@itn			int,				--iteration number identifier
		@max_itn		int,
		@min_itn		int,
		@itn_count		int,
		@error			int,
		@msg			varchar(100),
		@rtl_table		varchar(25),
		@bf_RERUN		int

select	@itn_size = isnull(@itn_size, 500000)	-- set default processing iteration size
select	@user_sysid = isnull(@user_sysid, -1),	-- if user id is not provided use default unknown user value
		@bf_RERUN = 0x10						-- bitflag to tell that we are doing rerun of TRX import processing
		
--initialize variables
select	@ret = 0,
		@rec_count	= 0,
		@error = 0

--do pre-requisite checks before starting processing

--ensure that process does not have unprocessed data remainders with intermediate state
if exists (select * from REC_DI_TRX_LOAD
				where EVENT_ID <>@event_id) 
			or exists (select * from CTL_DI_ACC_TRX_ITN
				where EVENT_ID <>@event_id)
	begin
	raiserror ('Previous processing session did not complete (REC_DI_TRX_LOAD and CTL_DI_ACC_TRX_ITN tables). Recovery is required, processing cannot continnue', 14, 1)
	end
else
	begin
		truncate table CTL_DI_ACC_TRX_ITN
		truncate table REC_DI_TRX_LOAD
	end
if @@error <> 0	
	goto ERR

exec	dbo.mp_PROGRESS_LOGGER	@event_id,
								'PREPARE',
								'Prepare records for processing',
								null,
								1

--check if processing has already been initiated
--if new process will have no records, if it is restart of interrupted process will have records
select	@holding_count = count(*)
from	CTL_DI_ACC_TRX

if @holding_count = 0
begin
	-- ensure that there are no null values in the key fields (this should be enforced by table details :TODO

	--get positions for processing
	insert	CTL_DI_ACC_TRX(
			GS,
			ITN,
			BF_RUN,
			ACT_SYSID,
			TRX_COUNT,
			EVENT_ID,
			UPDATE_DT,
			CREATE_DT
			)
	select	'READY',
			0,
			0,
			SDRT.ACT_SYSID,
			count(1),
			@event_id,
			getdate(),
			min(ACT.CREATE_DT)		
	from	SLT_DI_REC_TRX SDRT
	inner join ACT 
		on SDRT.ACT_SYSID = ACT.ACT_SYSID
	group by SDRT.ACT_SYSID

	select	@error = @@ERROR, @rec_count = @@ROWCOUNT
	if @error <> 0	goto ERR

	select	@msg =  'Total processing iterations=' + convert(varchar(14), (@max_itn - @min_itn + 1)) + ' | Transactions=' + convert(varchar(14), @trx_total) + ' | Holdings=' + convert(varchar(14), @holding_count)
		+ 'Holding selected: '+ convert(varchar(14), @rec_count)
		+ ' DI Event: ' + convert(varchar(12), @event_id)
exec	dbo.mp_PROGRESS_LOGGER	@event_id,
								'STAT0',
								@msg,
								null,
								1

	--allocate iteration identifiers to the records to run process in blocks defined by @itn_size
	exec	dbo.mp_DI_SET_ITERATIONS	'CTL_DI_ACC_TRX',
									@itn_size

	--get total number of transactions we need to process here
	select	@trx_total = count(*)
	from	CTL_DI_ACC_TRX

end


--get processing parameters, now that we have established what we are doing
select	@holding_count = count(*),	--number of positions to process
		@min_itn = min(ITN),		--first iteration number id
		@max_itn = max(ITN)			--last iteration number id
from	CTL_DI_ACC_TRX
where	GS = 'READY'



--if we have no records available for processing it stops here
if @holding_count = 0
begin
	exec	dbo.mp_PROGRESS_LOGGER	@event_id,
									'STAT',
									'No holding records outstanding for processing. Processing event will be set to completed',
									null,
									1
	goto DONE	
end
-- reset rec_count before iterations
select @rec_count = 0


select	@itn = @min_itn - 1,						--decrement iteration number id because of the structure of while loop condition below
		@itn_count = 0								--tracks how many iterations we did

select	@msg =  'Total processing iterations=' + convert(varchar(14), (@max_itn - @min_itn + 1)) + ' | Transactions=' + convert(varchar(14), @trx_total) + ' | Holdings=' + convert(varchar(14), @holding_count)
		+ ' DI Event: ' + convert(varchar(12), @event_id)
exec	dbo.mp_PROGRESS_LOGGER	@event_id,
								'STAT',
								@msg,
								null,
								1

--Iterations processing loop
while	(@max_itn - @itn > 0)
begin
	select	@itn = @itn + 1,				--this will control which iteration records we are going to pick up from the source (matching to the iteration id)
			@itn_count = @itn_count + 1		--this controls when we should stop the loop

	select	@msg = 'Start iteration: #' + convert(varchar, @itn_count) + ' for id:' + convert(varchar(15), @itn)
					+ ' DI Event: ' + convert(varchar(12), @event_id)
	exec	dbo.mp_PROGRESS_LOGGER	@event_id,
									'S_ITN',
									@msg,
									null,
									1

	-- TRX Recovery scenario check
	-- if @itn = 3 
	-- raiserror ('Error raised in third iteration', 14, 1)
	-- if @@error <> 0 goto ERR	

	--Clear up the table before loading next set
	truncate table CTL_DI_ACC_TRX_ITN
	truncate table REC_DI_TRX_LOAD					-- truncate REC_DI_TRX_LOAD before each iteration
	
	if @@error <> 0 goto ERR
	
	--take all the control records for given iteration and move them into the iteration specific table
	insert	CTL_DI_ACC_TRX_ITN(
			GS,
			ACT_SYSID,
			SOURCE,
			EVENT_ID
			)	
	select	GS,
			ACT_SYSID,
			'DITRX',
			@event_id
			
	from	CTL_DI_ACC_TRX
	where	CTL_DI_ACC_TRX.ITN = @itn
	and		CTL_DI_ACC_TRX.GS = 'READY'			--if this criteria were to change then insert statement below has to be changed as well


	select	@error = @@ERROR, @rec_count = @@ROWCOUNT
	if @error <> 0 goto ERR
	
	select	@msg =  'Total current iteration=' + ' | Holdings=' + convert(varchar(14), @rec_count)
		+ ' DI Event: ' + convert(varchar(12), @event_id)
	exec	dbo.mp_PROGRESS_LOGGER	@event_id,
								'STAT',
								@msg,
								null,
								1

	--copy records for processing into scrubbing worktable
	insert	REC_DI_TRX_LOAD(
			EVENT_ID,
			FAS_STAT,
			TRX_SEQ_NUM,
			TC_TRX_SEQ_NUM,
			TRX_CD,
			ACT_SYSID,
			TRX_SYSID,
			REP_SYSID,
			BRN_SYSID,
			RGN_SYSID,
			DLR_SYSID,
			IVD_LOAD_FLAG,
			TRX_REP_NUM,
			TRX_DLR_NUM,

			TRADE_DT,
			SETTLE_DT,
			TRX_PROC_DT,
			TRX_GRSS_AMT,
			TRX_FE_COMM,
			TRX_FE_HOLD,
			TRX_FE_DLR,
			TRX_DSC_COMM,
			TRX_DSC_DLR,
			TRX_FEES,
			TRX_GST,
			TRX_DEDU,
			TRX_WHLD_1,
			TRX_WHLD_2,
			TRX_NET_AMT,
			TRX_PAY_REC,
			TRX_PRICE,
			TRX_OPEN_BAL,
			TRX_UNIT,
			TRX_UNIT_ISS,
			TRX_UNIT_UISS,
			TRX_DILUTE_AMT,	
			TRX_MM_INT,
			TRX_AVG_DLR,
			TRX_AVG_MGT,
			TRX_WO_NUM,
			LSIF_PRV_TAX_CLAW,
			LSIF_FED_TAX_CLAW
			)
	select
			@event_id,
			'RT',				-- FAS_STAT,
			RTL.TRX_SEQ_NUM,		--DLR_SEQ_NUM,	-- TRX_SEQ_NUM
			RTL.TC_TRX_SEQ_NUM,		--TRX_SEQ_NUM,	-- TC_TRX_SEQ_NUM
			RTL.TRX_CD,			-- TRX_CD
			RTL.ACT_SYSID,		-- ACT_SYSID
			RTL.TRX_SYSID,		-- TRX_SYSID
			DSVA.REP_SYSID,		-- REP_SYSID,
			RTL.BRN_SYSID,		-- BRN_SYSID,
			RTL.RGN_SYSID,		-- RGN_SYSID
			RTL.DLR_SYSID,		-- DLR_SYSID
			DSVA.IVD_LOAD_FLAG,	-- IVD_LOAD_FLAG
			RTL.TRX_REP_NUM,		--REP_CD,			--TRX_REP_NUM,
			RTL.TRX_DLR_NUM,		--DLR_CD,			--TRX_DLR_NUM,

			RTL.TRADE_DT,		-- TRADE_DT,
			RTL.SETTLE_DT,		-- SETTLE_DT,
			RTL.TRX_PROC_DT,		-- TRX_PROC_DT
			RTL.TRX_GRSS_AMT,		--TRX_GROSS,				--TRX_GRSS_AMT
			RTL.TRX_FE_COMM,		--TRX_COMM_PCNT,		--TRX_FE_COMM
			RTL.TRX_FE_HOLD,		--TRX_HOLD_BACK,		-- TRX_FE_HOLD
			RTL.TRX_FE_DLR,			--TRX_COMM,			-- TRX_FE_DLR
			RTL.TRX_DSC_COMM,		--TRX_DSC_PCNT,		--TRX_DSC_COMM
			RTL.TRX_DSC_DLR,		--TRX_DSC_COMM,		-- TRX_DSC_DLR,
			RTL.TRX_FEES,			--TRX_ADM_FEE,		-- TRX_FEES,
			RTL.TRX_GST,			--TRX_WTH_GST,		-- TRX_GST,
			RTL.TRX_DEDU,			--TRX_WTH_DSC,		--TRX_DEDU,
			RTL.TRX_WHLD_1,			--TRX_WTH_FED,		-- TRX_WHLD_1,
			RTL.TRX_WHLD_2,			--TRX_WTH_PRV,		-- TRX_WHLD_2,
			RTL.TRX_NET_AMT,		--TRX_NET,			-- TRX_NET_AMT,
			RTL.TRX_PAY_REC,		--TRX_NET_PAY,		-- TRX_PAY_REC,
			RTL.TRX_PRICE,			-- TRX_PRICE,
			RTL.TRX_OPEN_BAL,		-- TRX_OPEN_BAL,
			RTL.TRX_UNIT,			-- TRX_UNIT,
			0,						-- TRX_UNIT_ISS,
			RTL.TRX_UNIT_UISS,		-- TRX_CLOSE_BAL, TRX_UNIT_UISS,
			RTL.TRX_DILUTE_AMT,		-- TRX_DILUTE_AMT
			RTL.TRX_MM_INT,			-- TRX_MM_INT
			RTL.TRX_AVG_DLR,		--TRX_AVG_DLR
			RTL.TRX_AVG_MGT,		--TRX_AVG_MGT
			RTL.TRX_WO_NUM,			-- TRX_WO_NUM
			RTL.LSIF_PRV_TAX_CLAW,	--LSIF_PRV_TAX_CLAW,
			RTL.LSIF_FED_TAX_CLAW	--LSIF_FED_TAX_CLAW
			
	from	SLT_DI_REC_TRX RTL 
		inner join CTL_DI_ACC_TRX_ITN 
			on 	RTL.ACT_SYSID = CTL_DI_ACC_TRX_ITN.ACT_SYSID
		inner join DI_STL_VERIFY_ACT DSVA
			on DSVA.ACT_SYSID = CTL_DI_ACC_TRX_ITN.ACT_SYSID
			and DSVA.BF_STATUS = 0
		
	select	@error = @@ERROR, @rec_count = @@ROWCOUNT
	if @@error <> 0 goto ERR
	
	select	@msg =  'Total current iteration=' + ' | TRX=' + convert(varchar(14), @rec_count)
		+ ' DI Event: ' + convert(varchar(12), @event_id)
	exec	dbo.mp_PROGRESS_LOGGER	@event_id,
								'STAT3',
								@msg,
								null,
								1

	-- update table statistics for performance improvement 
	update statistics REC_DI_TRX_LOAD with fullscan

	set @msg = 'Transaction Scrubbing Preparation completed'
	+ ' DI Event: ' + convert(varchar(12), @event_id)
	exec dbo.mp_PROGRESS_LOGGER		@event_id,
									'TSCRU4',
									@msg,		
									null,
									1

	--run transaction scrubbing processing
	
	exec @ret = dbo.mp_DI_REC_TRX_SCRUB_MAIN	@event_id,
											@user_sysid

	if @ret <> 0
		goto ERR

	-- TRX Recovery scenario check
	-- raiserror ('Error raised after completion of transaction scrubbing in mp_DI_IMPORT_TRX ', 14, 1)
    -- if @@error <> 0 goto ERR

	if @commit_flag = 1		--only if it is committing run will run this block
	begin
		raiserror ('Error raised after completion of transaction scrubbing in mp_DI_IMPORT_TRX @commit_flag=1 called in Analyze mode', 14, 1)
  

		if @ret <> 0
			goto ERR

		select	@msg = dbo.fp_LOG_PROCESS_NAME(15050, 'ENG') + ' completed for Event Id:' + convert(varchar(15), @stc_event_id)
		exec dbo.mp_PROGRESS_LOGGER		@event_id,
										'STCD',
										@msg,
										null,
										0

		select	@msg = 'Finalize iteration: #' + convert(varchar(15), @itn_count) + '. Trx candidate records processed so far: ' + convert(varchar(15), @rec_count)
		exec dbo.mp_PROGRESS_LOGGER		@event_id,
										'E_ITDI',
										@msg,
										null,
										1
												 
	end		--@commit_flag=1
	else
	begin
		select	@msg = 'Skipping process ' + dbo.fp_LOG_PROCESS_NAME(15050, 'ENG') + ' due to @commit_flag = 0'
		exec dbo.mp_PROGRESS_LOGGER		@event_id,
										'DISTCSC',
										@msg,
										null,
										1
	end		--@commit_flag=0

	-- TRX Recovery scenario check
	-- raiserror ('Error raised before update CTL_ACC_TRX in mp_DI_IMPORT_TRX  procedure', 14, 1)
	-- if @@error <> 0 goto ERR

	--Set status to successfully processed
	update	CTL_DI_ACC_TRX
	set		GS = case ITN.GS when 'READY' then 'OK' else ITN.GS end,										--this status will be updated by scrubbing and conversion procedures
			BF_RUN = isnull(CTL_DI_ACC_TRX.BF_RUN,0) | case ITN.GS when 'READY' then 0 else @bf_RERUN end,		--if record is exception it is tagged as rerun candidate
			MIN_TRX_PROC_DT = ITN.MIN_TRX_PROC_DT,															--copy over all fields from CTR_ACC_TRX_ITN used for validation to then use it in exceptions reporting
			MAX_TRX_PROC_DT = ITN.MAX_TRX_PROC_DT,
			MAX_TRX_SEQ_NUM = ITN.MAX_TRX_SEQ_NUM,
			MIN_TRX_SEQ_NUM = ITN.MIN_TRX_SEQ_NUM,
			ACT_SYSID = ITN.ACT_SYSID,
			ACT_UNIT = ITN.ACT_UNIT,
			ROLLUP_TRX_UNIT = ITN.ROLLUP_TRX_UNIT,
			FIRST_TRX_ARC_CTR = ITN.FIRST_TRX_ARC_CTR,
			LAST_TRX_ARC_CTR = ITN.LAST_TRX_ARC_CTR,
			FIRST_TRX_OPEN_BAL = ITN.FIRST_TRX_OPEN_BAL,
			LAST_TRX_CLOSE_BAL = ITN.LAST_TRX_CLOSE_BAL,
			TRX_UNIT_RU_POST_MPD = ITN.TRX_UNIT_RU_POST_MPD,
			TRX_UNIT_RU_PRE_MPD	= ITN.TRX_UNIT_RU_PRE_MPD
	from	CTL_DI_ACC_TRX inner join CTL_DI_ACC_TRX_ITN ITN 
			on 	CTL_DI_ACC_TRX.ACT_SYSID = ITN.ACT_SYSID
	where	CTL_DI_ACC_TRX.ITN = @itn												--it may look wierd to include iteration # here because join criteria should be unique but for consistency 
																				--sake it makes sense to do so

	select	@error = @@ERROR
	if @error <> 0	goto ERR
		
end		--while loop

select	@msg = 'Processing completed in ' + convert(varchar(15), @itn_count) + ' iterations, processing: ' + convert(varchar(15),@rec_count) + ' transaction candidate records'
exec dbo.mp_PROGRESS_LOGGER		@event_id,
								'END',
								@msg,	
								null,
								1


--Cleanup iteration table after last iteration to prevent false recovery conditions
--truncate table CTL_DI_ACC_TRX_ITN
--truncate table REC_DI_TRX_LOAD


--check to ensure we have processed all records
select	@rec_count = 0

-- updating Primary DI table for the itertion
select	@rec_count = count(*) 
from	CTL_DI_ACC_TRX
where	CTL_DI_ACC_TRX.GS = 'READY'

if @rec_count > 0 
	select	@rec_count 'Number of holding records left to process (should be zero)'

goto DONE


ERR:
	select	@msg = 'Error in processing: Last procedure return code=' + convert(varchar(15), @ret) + ', last error=' + convert(varchar(15),@error) + '. Processing terminated'
	raiserror (@msg, 14, 1)
	return(@@error)

DONE:

return(0)
end
go

grant execute on dbo.mp_DI_IMPORT_TRX to MPSMain

exec mt_RM_VT 'mp_DI_IMPORT_TRX', '$Id: mp_DI_IMPORT_TRX.sql 124282 2015-02-24 18:46:07Z dge $'
go