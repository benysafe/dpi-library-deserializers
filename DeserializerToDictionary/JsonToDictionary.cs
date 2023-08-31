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

namespace DeserializerJsonToDictionary
{
    public class JsonToDictionary : IDeserializer
    {
        private IProcessor _Processor;
        private Logger _logger;
        private string _id;
        IConfigurator _configurator;

        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _id = id;
                _configurator = configurator;
                _logger = (Logger)logger.init("DeserializerJsonToDictionary");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

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

        public bool deserialize(object payload, Dictionary<string, object> metadata)
        {
            try
            {
                _logger.Trace("Inicio");
                string strPayload = Encoding.UTF8.GetString((byte[])payload);    
                Dictionary<string, object> dicPayload = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(strPayload);
                string strMetadata = null;
                if (metadata != null)
                {
                    strMetadata = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(metadata);
                }
                _logger.Debug("  Dato '{0}'      Metadato  '{1}'", strPayload, strMetadata);

                if (!_Processor.proccess(dicPayload, metadata))
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
    }
}
