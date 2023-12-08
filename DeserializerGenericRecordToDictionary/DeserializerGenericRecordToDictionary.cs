using InterfaceLibraryDeserializer;
using InterfaceLibraryProcessor;
using InterfaceLibraryConfigurator;
using Definitions;
using System;
using System.Text.Json.Serialization;
using System.Collections.Generic;
using System.Threading;
using InterfaceLibraryLogger;
using NLog;
using System.Text;
using Avro.Generic;
using Avro;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka;

namespace DeserializerGenericRecordToDictionary

{
    public class DeserializerGenericRecordToDictionary : IDeserializer
    {
        private IProcessor _Processor;
        private Logger _logger;
        private string _id;
        IConfigurator _configurator;

        public void addProcessor(IProcessor processor)
        {
            try
            {
                _Processor = processor;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public bool deserialize(object payload, Dictionary<string, object> metaDatas = null)
        {
            try
            {
                _logger.Trace("Inicio");
                bool error = false;
                Dictionary<string, object> dicPayload = ConvertToDictionary((GenericRecord)payload, ref error);
                if (dicPayload != null || error == true) 
                {
                    _logger.Error($"No fue posible ejecutar la deserialización del mensaje");
                    return false;
                }
                string strPayload = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dicPayload);
                string strMetadata = null;
                if (metaDatas != null)
                {
                    strMetadata = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(metaDatas);
                }
                _logger.Debug("  Dato '{0}'      Metadato  '{1}'", strPayload, strMetadata);

                if (!_Processor.proccess((object)dicPayload, (object)metaDatas))
                {
                    _logger.Error($"No se ejecuto el procesamiento del mensaje: {strPayload}");
                    return false;
                }
                _logger.Trace("Fin");
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                return false;
                throw ex;
            }
        }

        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _id = id;
                _configurator = configurator;
                _logger = (Logger)logger.init("DeserializerGenericRecordToDictionary");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        private Dictionary<string, object> ConvertToDictionary(GenericRecord record, ref bool error)
        {
            try
            {
                _logger.Trace("Inicio");
                Dictionary<string, object> dicOut = new Dictionary<string, object>();

                foreach (Field field in record.Schema.Fields)
                {
                    if (record[field.Name].GetType().FullName == "Avro.Generic.GenericRecord")
                    {
                        bool subError = false;
                        dicOut.Add(field.Name, ConvertToDictionary(record[field.Name] as GenericRecord, ref subError));
                        if(subError)
                        {
                            error = true;
                            return null;
                        }
                    }
                    else
                    {
                        dicOut.Add(field.Name, record[field.Name]);
                    }
                }
                _logger.Trace("Fin");
                return dicOut;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
    }
}