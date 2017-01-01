package com.cmbchina;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Andrew on 29/12/2016.
 */
public class TableProperty {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private List<ColumnProperty> columnProperties;
    private String tabschema;
    private String tabname;
    private String sql;
    private JdbcTemplate jdbcTemplate;

    public TableProperty(JdbcTemplate jdbcTemplate,String tabschema,String tabname){
        this.columnProperties=new ArrayList<>();
        this.tabschema=tabschema;
        this.tabname=tabname;
        this.jdbcTemplate = jdbcTemplate;
        this.MakeSQL();
    }

    public String getSql() {
        return this.sql;
    }

    public void MakeSQL(){
        try {
            this.columnProperties=this.findColunmProperty();
            StringBuffer mark = new StringBuffer();
            StringBuffer updateSql = new StringBuffer();
            StringBuffer insertColunms = new StringBuffer();
            StringBuffer insertColunmT  = new StringBuffer();
            StringBuffer conditionSql = new StringBuffer();
        for(int i=0;i<columnProperties.size();i++){
            ColumnProperty columnProperty=columnProperties.get(i);
            String colunm = columnProperty.getColName();
            if(columnProperty.getKeySeq()>0)
            {
                if(null!=conditionSql.toString()&&!conditionSql.toString().isEmpty())
                {
                    conditionSql.append(" AND T."+colunm+"="+"S."+colunm);
                }
                else
                {
                    conditionSql.append("T."+colunm+"="+"S."+colunm);
                }
            }
            if(i>0)
            {
                mark.append(",");
                updateSql.append(",");
                insertColunms.append(",");
                insertColunmT.append(",");
            }
            mark.append("?");
            updateSql.append("T."+colunm+"="+"S."+colunm);
            insertColunms.append("S."+colunm);
            insertColunmT.append(colunm);
        }
            StringBuffer sb = new StringBuffer();
            sb.append("/** DATA INPUT **/ MERGE INTO "+this.tabschema.toUpperCase()+".");
            sb.append(this.tabname.toUpperCase());
            sb.append(" AS T USING TABLE(VALUES(");
            sb.append(mark);
            sb.append(")) S (");
            sb.append(insertColunmT);
            sb.append(") ON (");
            sb.append(conditionSql);
            sb.append(") WHEN MATCHED THEN UPDATE SET ");
            sb.append(updateSql);
            sb.append(" WHEN NOT MATCHED THEN INSERT (");
            sb.append(insertColunmT);
            sb.append(") VALUES (");
            sb.append(insertColunms);
            sb.append(")");
            this.sql=sb.toString();
            logger.info(this.sql);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public List<ColumnProperty> getColumnProperties(){
        return this.columnProperties;
    }

    private List<ColumnProperty> findColunmProperty() throws Exception
    {
        String sql = "SELECT COLNO,KEYSEQ,TYPENAME,COLNAME,NULLS,LENGTH FROM SYSCAT.COLUMNS  WHERE TABSCHEMA=? AND TABNAME=? AND ROWCHANGETIMESTAMP<>'Y' AND HIDDEN!='I' ORDER BY COLNO ASC WITH UR";
        return  this.jdbcTemplate.query(sql, new RowMapper<ColumnProperty>() {
            @Override
            public ColumnProperty mapRow(ResultSet resultSet, int i) throws SQLException {
                ColumnProperty columnProperty = new ColumnProperty(resultSet.getString("COLNAME"),resultSet.getString("TYPENAME"),resultSet.getInt("COLNO"),resultSet.getInt("KEYSEQ"),resultSet.getBoolean("NULLS"),resultSet.getInt("LENGTH"));
                return columnProperty;
            }
        }, this.tabschema.toUpperCase(), this.tabname.toUpperCase());
    }

    public String getTabschema() {
        return tabschema;
    }

    public String getTabname() {
        return tabname;
    }
}