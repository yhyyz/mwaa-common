select 'ds' , '{{ ds }}';
select 'ds' , '{{ ds_nodash }}';
select 'macros.ds_add-1' , '{{ macros.ds_add(ds,-1) }}';
select 'macros.ds_format' ,'{{ macros.ds_format(ds, "%Y-%m-%d", "%m-%d-%y") }}';
select 'macros.ds_format-1', '{{ macros.ds_format(macros.ds_add(ds,-1), "%Y-%m-%d", "%m-%d-%y") }}';
select 'execution_date' , '{{ execution_date }}';
select 'execution_date format' , '{{ execution_date.strftime("%d-%m-%Y") }}';
select 'execution_date-5 format', '{{ (execution_date - macros.timedelta(days=5)).strftime("%Y-%m-%d") }}';
select 'data_interval strftime', '{{ data_interval_start.strftime("%d-%m-%Y") }}', '{{ data_interval_end.strftime("%d-%m-%Y") }}';
select 'biz_date' ,'{{ biz_date(data_interval_end, days=-1) }}';