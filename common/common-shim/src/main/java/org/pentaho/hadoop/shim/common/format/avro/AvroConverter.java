/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.hadoop.shim.common.format.avro;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaInternetAddress;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

/**
 * Created by rmansoor on 10/7/2017.
 */
public class AvroConverter {

  public static GenericRecord convertToAvro( RowMetaAndData row, Schema schema, SchemaDescription schemaDescription ) {
    RowMetaInterface rmi = row.getRowMeta();
    GenericRecord outputRecord = new GenericData.Record( schema );

    try {
      //Build the avro row
      for ( SchemaDescription.Field field : schemaDescription ) {
        int fieldMetaIndex = rmi.indexOfValue( field.pentahoFieldName );
        ValueMetaInterface vmi = rmi.getValueMeta( fieldMetaIndex );
        switch ( vmi.getType() ) {
          case ValueMetaInterface.TYPE_INET:
          case ValueMetaInterface.TYPE_STRING:
            outputRecord.put( field.formatFieldName, row.getString( fieldMetaIndex,
                String.valueOf( field.defaultValue ) ) );
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            if ( field.defaultValue != null && field.defaultValue.length() > 0 ) {
              outputRecord.put( field.formatFieldName, row.getInteger( fieldMetaIndex,
                  Long.parseLong( field.defaultValue ) ) );
            } else {
              outputRecord.put( field.formatFieldName, row.getInteger( fieldMetaIndex ) );
            }
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            outputRecord.put( field.formatFieldName, row.getNumber( fieldMetaIndex,
                ( field.defaultValue != null && field.defaultValue.length() > 0 )
                    ? Double.parseDouble( field.defaultValue ) : 0 ) );
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            if ( field.defaultValue != null && field.defaultValue.length() > 0 ) {
              BigDecimal defaultBigDecimal = new BigDecimal( field.defaultValue );
              BigDecimal bigDecimal = row.getBigNumber( fieldMetaIndex, defaultBigDecimal );
              outputRecord.put( field.formatFieldName, bigDecimal.doubleValue() );
            } else {
              BigDecimal bigDecimal = row.getBigNumber( fieldMetaIndex, null );
              if ( bigDecimal != null ) {
                outputRecord.put( field.formatFieldName, bigDecimal.doubleValue() );
              } else {
                outputRecord.put( field.formatFieldName, null );
              }
            }
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            Date defaultTimeStamp = null;
            if ( field.defaultValue != null && field.defaultValue.length() > 0 ) {
              DateFormat dateFormat = new SimpleDateFormat( vmi.getConversionMask() );
              try {
                defaultTimeStamp = dateFormat.parse( field.defaultValue );
              } catch ( ParseException pe ) {
                defaultTimeStamp = null;
              }
            }
            Date timeStamp =  row.getDate( fieldMetaIndex, defaultTimeStamp );
            outputRecord.put( field.formatFieldName, timeStamp.getTime() );
            break;
          case ValueMetaInterface.TYPE_DATE:
            Date defaultDate = null;
            if ( field.defaultValue != null && field.defaultValue.length() > 0 ) {
              DateFormat dateFormat = new SimpleDateFormat( vmi.getConversionMask() );
              try {
                defaultDate = dateFormat.parse( field.defaultValue );
              } catch ( ParseException pe ) {
                defaultDate = null;
              }
            }
            Date dateFromRow =  row.getDate( fieldMetaIndex, defaultDate );
            LocalDate rowDate = dateFromRow.toInstant().atZone( ZoneId.systemDefault() ).toLocalDate();
            outputRecord.put( field.formatFieldName, Math.toIntExact( ChronoUnit.DAYS.between( LocalDate.ofEpochDay( 0 ), rowDate ) ) );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            outputRecord.put( field.formatFieldName, row.getBoolean( fieldMetaIndex,
                Boolean.parseBoolean( field.defaultValue ) ) );
            break;
          case ValueMetaInterface.TYPE_BINARY:
            if ( field.defaultValue != null && field.defaultValue.length() > 0 ) {
              outputRecord.put( field.formatFieldName, ByteBuffer.wrap( row.getBinary( fieldMetaIndex,
                  vmi.getBinary( field.defaultValue.getBytes() ) ) ) );
            } else {
              outputRecord.put( field.formatFieldName, ByteBuffer.wrap( row.getBinary( fieldMetaIndex, new byte[0] ) ) );
            }
            break;
          default:
            break;
        }
      }
    } catch ( ArithmeticException e ) {
      throw new IllegalArgumentException( "The date has too much day from epoch day!", e );
    } catch ( KettleValueException e ) {
      throw new IllegalArgumentException( "some exception while writing avro", e );
    }
    return outputRecord;
  }

  public static RowMetaAndData convertFromAvro( GenericRecord record, SchemaDescription schemaDescription ) {
    RowMeta fields = new RowMeta();
    RowMetaAndData rowMetaAndData = new RowMetaAndData(  );
    int index = 0;

    for ( SchemaDescription.Field field : schemaDescription ) {
      switch ( field.pentahoValueMetaType ) {
        case ValueMetaInterface.TYPE_INET:
          fields.addValueMeta( new ValueMetaInternetAddress( field.formatFieldName ) );
          rowMetaAndData.getData()[ index++ ] = record.get( field.formatFieldName );
          break;
        case ValueMetaInterface.TYPE_STRING:
          fields.addValueMeta( new ValueMetaString( field.formatFieldName ) );
          rowMetaAndData.getData()[ index++ ] = record.get( field.formatFieldName );
          break;
        case ValueMetaInterface.TYPE_INTEGER:
          fields.addValueMeta( new ValueMetaInteger( field.formatFieldName ) );
          rowMetaAndData.getData()[ index++ ] = record.get( field.formatFieldName );
          break;
        case ValueMetaInterface.TYPE_NUMBER:
          fields.addValueMeta( new ValueMetaNumber( field.formatFieldName ) );
          rowMetaAndData.getData()[ index++ ] = record.get( field.formatFieldName );
          break;
        case ValueMetaInterface.TYPE_BIGNUMBER:
          fields.addValueMeta( new ValueMetaBigNumber( field.formatFieldName ) );
          BigDecimal bigDecimal = new BigDecimal( Double.parseDouble( (String) record.get( field.formatFieldName ) ) );
          rowMetaAndData.getData()[ index++ ] = bigDecimal;
          break;
        case ValueMetaInterface.TYPE_TIMESTAMP:
          fields.addValueMeta( new ValueMetaTimestamp( field.formatFieldName ) );
          Long longTimeStamp = (Long) record.get( field.formatFieldName );
          rowMetaAndData.getData()[ index++ ] = new Date( longTimeStamp );
          break;
        case ValueMetaInterface.TYPE_DATE:
          fields.addValueMeta( new ValueMetaDate( field.formatFieldName ) );
          Integer dateAsInteger  = (Integer) record.get( field.formatFieldName );
          LocalDate localDate = LocalDate.ofEpochDay( 0 ).plusDays( dateAsInteger );
          rowMetaAndData.getData()[ index++ ] = Date.
              from( localDate.atStartOfDay( ZoneId.systemDefault() ).toInstant() );
          break;
        case ValueMetaInterface.TYPE_BOOLEAN:
          fields.addValueMeta( new ValueMetaBoolean( field.formatFieldName ) );
          rowMetaAndData.getData()[ index++ ] = record.get( field.formatFieldName );
          break;
        case ValueMetaInterface.TYPE_BINARY:
          fields.addValueMeta( new ValueMetaBinary( field.formatFieldName ) );
          ByteBuffer byteBuffer  = (ByteBuffer) record.get( field.formatFieldName );
          byte[] byteArray = new byte[byteBuffer.remaining()];
          rowMetaAndData.getData()[ index++ ] = byteBuffer.get( byteArray );
          break;
        default:
          throw new RuntimeException( "Field: " + field.formatFieldName + "  Undefined type: " + field.pentahoValueMetaType );
      }
    }
    return rowMetaAndData;
  }
}
