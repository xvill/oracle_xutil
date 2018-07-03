
--==============================================================================================

CREATE TABLE SYS_DATAPART
  (    ID VARCHAR2(40) DEFAULT SYS_GUID() NOT NULL ENABLE,
    OWNER VARCHAR2(30) NOT NULL ENABLE,
    TABNAME VARCHAR2(30) NOT NULL ENABLE,
    COLNAME VARCHAR2(30) NOT NULL ENABLE,
    TIMEUNIT VARCHAR2(10) NOT NULL ENABLE,
    REMAINDAYS NUMBER(*,0) NOT NULL ENABLE,
    TBSNAME VARCHAR2(30) NOT NULL ENABLE,
    PARTOPTIONS VARCHAR2(200) NOT NULL ENABLE,
    PREDAYS NUMBER(*,0) NOT NULL ENABLE,
    STATUS VARCHAR2(30) NOT NULL ENABLE
  );


-- 分区维护任务
begin
 dbms_scheduler.create_job (
    job_name           =>'JOB_DATACHECK',
    job_type           =>'PLSQL_BLOCK',
    job_action         =>'begin db_toolkit.part_check(); end;',
    repeat_interval    =>'FREQ=DAILY;BYHOUR=21,22,23,0,1,2,3,4;BYMINUTE=10;BYSECOND=0',
    start_date         => sysdate
    );
 dbms_scheduler.enable('JOB_DATACHECK');
end;
--==============================================================================================
  CREATE TABLE "SYS_LOG"
   (    "ID" VARCHAR2(40) DEFAULT SYS_GUID() NOT NULL ENABLE,
    "XTIME" DATE DEFAULT SYSDATE NOT NULL ENABLE,
    "DURATION" VARCHAR2(40),
    "OWNER" VARCHAR2(100) DEFAULT USER NOT NULL ENABLE,
    "TYPE" VARCHAR2(200),
    "NAME" VARCHAR2(200),
    "PARAMS" VARCHAR2(2000),
    "STATUS" NUMBER(*,0),
    "INFO" VARCHAR2(2000),
    "COMM" VARCHAR2(200)
   );
/
create or replace package db_toolkit is

  -- Author  : willv
  -- Created : 2017/2/8 10:53:24
  -- Purpose : 系统工具包

  procedure log_begin(i_logid out varchar2
                    ,i_params varchar2 default null
                    ,i_comm varchar2 default null
                    ,i_info varchar2 default null);

  procedure log_exception(i_logid out varchar2
            ,i_params varchar2 default null
            ,i_status number default 0
            ,i_info varchar2 default 'SUCCEEDED'
            ,i_comm varchar2 default null
            );

  procedure log_end(i_logid varchar2,i_status number default 0,i_info varchar2 default 'SUCCEEDED');

  procedure part_add(i_tabname varchar2,i_part_type varchar2,i_fromdate date,i_predays integer
                                        ,i_tbs  varchar2 default null,i_options varchar2 default 'NOLOGGING');

  procedure part_split(i_tabname varchar2,i_part_type varchar2,i_fromdate date ,i_splitdays integer);

  procedure part_check(i_date date default sysdate);

  procedure tab_backup(i_tab  varchar2,
                      i_2tab varchar2,
                      i_owner  varchar2 default user,
                      i_2owner  varchar2 default user);
end ;
/

create or replace package body db_toolkit is
  procedure log_begin(i_logid out varchar2
                    ,i_params varchar2 default null
                    ,i_comm varchar2 default null
                    ,i_info varchar2 default null) as
     v_owner    varchar2 (30);
     v_name     varchar2 (30);
     v_lineno   number;
     v_type     varchar2 (30);
     v_sub      varchar2 (50);
  begin
    i_logid :=  sys_guid();
    owa_util.who_called_me (v_owner, v_name, v_lineno, v_type);

  --获取包的子名称
   if v_type ='PACKAGE BODY' then
    with sd as (
      select name,type,line,trim(replace(replace(upper(substr(text,1,instr(text,'(')-1)),'PROCEDURE',''),'FUNCTION','')) sub
       from user_source where  name=v_name and type='PACKAGE BODY' and
      (instr(trim(upper(text)),'PROCEDURE')=1 or instr(trim(upper(text)),'FUNCTION')=1) and line < v_lineno
      )
      select '.'||sub into v_sub from sd where line =(select max(line) from sd);
    else
      v_sub :='';
    end if;

    insert into sys_log (id,xtime,owner,type,name,params,comm,status,info,duration)
      values(i_logid,sysdate,v_owner,v_type,lower(v_name||v_sub),i_params,i_comm,null,i_info,null);
    commit;
  end;

  procedure log_exception(i_logid out varchar2
            ,i_params varchar2 default null
            ,i_status number default 0
            ,i_info varchar2 default 'SUCCEEDED'
            ,i_comm varchar2 default null
            ) as
     v_owner    varchar2 (30);
     v_name     varchar2 (30);
     v_lineno   number;
     v_type     varchar2 (30);
     v_sub      varchar2 (50);
  begin
    if i_logid is null then
        --获取包的子名称
        i_logid :=  sys_guid();
        owa_util.who_called_me (v_owner, v_name, v_lineno, v_type);     
       if v_type ='PACKAGE BODY' then
            with sd as (
              select name,type,line,trim(replace(replace(upper(substr(text,1,instr(text,'(')-1)),'PROCEDURE',''),'FUNCTION','')) sub
               from user_source where  name=v_name and type='PACKAGE BODY' and
              (instr(trim(upper(text)),'PROCEDURE')=1 or instr(trim(upper(text)),'FUNCTION')=1) and line < v_lineno
              )
              select '.'||sub into v_sub from sd where line =(select max(line) from sd);
            else
              v_sub :='';
            end if;
        insert into sys_log (id,xtime,owner,type,name,params,comm,status,info,duration)
          values(i_logid,sysdate,v_owner,v_type,lower(v_name||v_sub||'_'||v_lineno),i_params,i_comm,i_status,i_info,0);
    else
        update sys_log set status=i_status ,info=i_info, duration= substr(numtodsinterval(sysdate - xtime,'day'),10,10) where id=i_logid;
    end if;
    commit;
  END;
  procedure log_end(i_logid varchar2,i_status number default 0,i_info varchar2 default 'SUCCEEDED') as
  begin
    update sys_log set status=i_status ,info=i_info, duration= substr(numtodsinterval(sysdate - xtime,'day'),10,10) where id=i_logid;
    commit;
  END;
  --=================================================================================================
procedure part_add(i_tabname varchar2,i_part_type varchar2,i_fromdate date ,i_predays integer
                    ,i_tbs  varchar2 default null,i_options varchar2 default 'NOLOGGING')
    as
      v_logid     varchar2(40);
      v_params    varchar(2000);
      v_partname  varchar2(32);
      v_partmax   varchar2(32);
      v_partmaxdate date;
      v_fromdate date;
      v_partbegindate date;
      v_partenddate date;
      v_predays number;
      v_sql           varchar2(3000);
      v_timechar      varchar2(32);
      v_unit          number;
    begin
        ------------------------LOG----------------------------------------------
      v_params := i_tabname||' , '||i_part_type
                  ||' , '||to_char(i_fromdate,'yyyy-mm-dd_hh24')
                  ||' , '||i_predays
                  ||' , '||nvl(i_tbs,'null')
                  ||' , '||nvl(i_options,'null');

      db_toolkit.log_begin(v_logid,v_params);
        ------------------------MAIN BEGIN----------------------------------------------
      select max(subobject_name) into v_partname from user_objects
              where object_name=upper(i_tabname) and object_type='TABLE PARTITION';
      if i_part_type='DD' then
        v_unit := 1;
      elsif  i_part_type='HH24' then
        v_unit := 1/24;
      elsif  i_part_type='IW' then
        v_unit := 7;
      end if;

       v_partmaxdate:=to_date(substr(v_partname,-10),'yyyymmddhh24');
       v_partenddate := i_fromdate+ i_predays;
       v_fromdate :=greatest(v_partmaxdate,i_fromdate);
       v_predays := v_partenddate - v_fromdate;

       if v_predays>0 then
           for v_index in 0 .. v_predays/v_unit loop
             v_partbegindate:=trunc(v_fromdate,'DD') + v_index*v_unit;
             if(v_partbegindate >v_partmaxdate) then
                v_partmax :=to_char(v_partbegindate,'yyyymmddhh24');
                v_timechar :=to_char(v_partbegindate + v_unit,'yyyy-mm-dd hh24:mi:ss');
                v_sql :='alter table '||i_tabname||' add partition P_'||v_partmax||' values less than(to_date('''
                       ||v_timechar||''', ''syyyy-mm-dd hh24:mi:ss'', ''nls_calendar=gregorian'')) '
                       ||case when i_tbs is null then ''  else ' tablespace '||i_tbs end
                       ||' '||i_options;
                --dbms_output.put_line(v_sql);
                execute immediate v_sql;
               end if;
           end loop;
         end if;
        ------------------------MAIN END----------------------------------------------
        db_toolkit.log_end(v_logid);
    exception
      when others then
        rollback;
        db_toolkit.log_exception(v_logid,v_params,sqlcode, sqlerrm);
    end ;

procedure part_drop(i_tab in varchar2,i_expireddate in date) as
    v_logid     varchar2(40);
    v_params    varchar(2000);
    v_expiredpartname varchar2(32);
    begin
        ------------------------LOG----------------------------------------------
      v_params := i_tab||' , '||to_char(i_expireddate,'yyyy-mm-dd_hh24');
      db_toolkit.log_begin(v_logid,v_params);

        ------------------------MAIN BEGIN----------------------------------------------
      v_expiredpartname:='P_'||to_char(i_expireddate,'yyyymmdd')||'00';

        for v_row in (
            select 'alter table '||object_name||' drop partition ' ||subobject_name dropsql
                   from user_objects where object_name=upper(i_tab)
                        and object_type='TABLE PARTITION' and  subobject_name <= v_expiredpartname
            ) loop
            --dbms_output.put_line(v_sql);
            execute immediate v_row.dropsql;
        end loop;
        ------------------------MAIN END----------------------------------------------
            db_toolkit.log_end(v_logid);
        exception
          when others then
            rollback;
            db_toolkit.log_exception(v_logid,v_params,sqlcode, sqlerrm);
        end ;

  procedure part_split(i_tabname varchar2,i_part_type varchar2,i_fromdate date ,i_splitdays integer) as
      v_logid     varchar2(40);
      v_params    varchar(2000);
      v_partnext   varchar2(32);
      v_partname  varchar2(32);
      v_partsplit  varchar2(32);
      v_partmindate date;
      v_time date;
      v_fromdate date;
      v_predays number;
      v_sql           varchar2(3000);
      v_timechar      varchar2(32);
      v_unit          number;
      v_exists          number;
      
    begin
        ------------------------LOG----------------------------------------------
      v_params := i_tabname||' , '||i_part_type
                  ||' , '||to_char(i_fromdate,'yyyy-mm-dd_hh24')
                  ||' , '||i_splitdays ;
                        
      db_toolkit.log_begin(v_logid,v_params);
        ------------------------MAIN BEGIN----------------------------------------------
        if i_part_type='DD' then
          v_unit := 1;
        elsif  i_part_type='HH24' then
          v_unit := 1/24;
        elsif  i_part_type='IW' then
          v_unit := 7;
        end if;

      select min(subobject_name) into v_partname from user_objects
              where object_name=upper(i_tabname) and object_type='TABLE PARTITION';
      if i_part_type='DD' then
        v_unit := 1;
      elsif  i_part_type='HH24' then
        v_unit := 1/24;
      elsif  i_part_type='IW' then
        v_unit := 7;
      end if;

       v_partmindate:=to_date(substr(v_partname,-10),'yyyymmddhh24');
       v_fromdate := greatest(v_partmindate,i_fromdate);
       v_predays := v_fromdate - trunc(i_fromdate - i_splitdays,'DD')  ;
            
       for v_index in 0 .. v_predays/v_unit loop
          v_time:=trunc(v_fromdate,i_part_type) - v_index*v_unit;
          v_timechar := to_char(v_time+ v_unit,'yyyy-mm-dd hh24:mi:ss');
          v_partsplit:= 'P_'||to_char(v_time+ v_unit,'yyyymmddhh24');
          v_partnext:= 'P_'||to_char(v_time,'yyyymmddhh24');
          select  count(*) into v_exists from user_objects
              where object_name=upper(i_tabname) and object_type='TABLE PARTITION' and SUBOBJECT_NAME= v_partnext;
          if v_exists = 0 then            
              v_sql :='alter table '||i_tabname||' split partition '||v_partsplit||' at ( to_date('''||v_timechar
                             ||''', ''syyyy-mm-dd hh24:mi:ss'', ''nls_calendar=gregorian''))  into (partition '
                             ||v_partnext||' , partition '||v_partsplit||')';
              --dbms_output.put_line('prompt '||v_count||'['||v_tabname||']['||v_timechar||']');
              --dbms_output.put_line(v_sql);
              execute immediate v_sql;
          end if ;
         end loop;
        ------------------------MAIN END----------------------------------------------
        db_toolkit.log_end(v_logid);
    exception
      when others then
        rollback;
        db_toolkit.log_exception(v_logid,v_params,sqlcode, sqlerrm);
    end ;

  procedure part_check(i_date date default sysdate) as
    v_logid     varchar2(40);
    v_params    varchar(2000);
    v_expireddate       date;
  begin
        ------------------------LOG----------------------------------------------
      v_params := to_char(i_date,'yyyy-mm-dd_hh24');
      db_toolkit.log_begin(v_logid,v_params);

        ------------------------MAIN BEGIN----------------------------------------------
        for v_row in ( select owner,tabname,timeunit
                ,remaindays,tbsname,partoptions,predays from sys_datapart where status='VALID'
        ) loop
          db_toolkit.part_add(v_row.tabname,v_row.timeunit,i_date,v_row.predays,v_row.tbsname,v_row.partoptions);

          v_expireddate:=i_date-v_row.remaindays;
          db_toolkit.part_drop(v_row.tabname,v_expireddate);
        end loop;

        ------------------------MAIN END----------------------------------------------
        db_toolkit.log_end(v_logid);
    exception
      when others then
        rollback;
        db_toolkit.log_exception(v_logid,v_params,sqlcode, sqlerrm);
    end ;
--===========================================================================================

  procedure tab_backup(i_tab  varchar2,
                      i_2tab varchar2,
                      i_owner  varchar2 default user,
                      i_2owner  varchar2 default user) as

        v_logID   varchar2(40);
        v_params  varchar2(2000) := 'i_tab={0},i_2tab={1},i_owner=>{2},i_2owner={3}';

        v_sql      varchar2(2000);
        v_partanme varchar2(100);
        v_bkver    varchar2(100);
        v_bkdate   date;
      begin
        ------------------------LOG----------------------------------------------
        v_params := replace(v_params, '{0}', i_tab);
        v_params := replace(v_params, '{1}', i_2tab);
        v_params := replace(v_params, '{2}', i_owner);
        v_params := replace(v_params, '{3}', i_2owner);
        db_toolkit.log_begin(v_logid,v_params);
        ------------------------MAIN BEGIN----------------------------------------------
        v_bkdate   := sysdate;
        v_partanme := to_char(v_bkdate, 'yyyymmdd_hh24miss');
        v_bkver    := to_char(v_bkdate, 'yyyy-mm-dd hh24:mi:ss');
        v_sql      := 'alter table ' || i_2owner || '.' || i_2tab ||
                      ' add  PARTITION P' || v_partanme || '  VALUES (''' || v_bkver || ''') ';
        --dbms_output.put_line(v_sql);
        execute immediate v_sql;
        v_sql := 'insert into ' || i_2owner || '.' || i_2tab ||
                 ' select ''' || v_bkver || ''',t.* from ' || i_owner || '.' || i_tab || ' t';
        --dbms_output.put_line(v_sql);
        execute immediate v_sql;
        commit;
        ------------------------MAIN END----------------------------------------------
        db_toolkit.log_end(v_logid);
    exception
      when others then
        rollback;
        db_toolkit.log_exception(v_logid,v_params,sqlcode, sqlerrm);
    end ;


end ;
/

