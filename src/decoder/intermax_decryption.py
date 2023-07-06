import jnius_config
from src.decoder.intermax_decoding import DecodingJs
from src.common.constants import SystemConstants
import glob


class Decoding:

    def __init__(self, config):
        self.config = config
        self.decryption_class = None
    def set_path(self):
        java_decode_path = f"{self.config['home']}"+SystemConstants.DECODING_JAR_FILE_PATH
        java_resource_jar_path = ''.join(glob.glob(java_decode_path + '/*jar'))

        jnius_config.add_options('-Xms128m', '-Xmx256m')
        jnius_config.set_classpath(java_resource_jar_path)

        from jnius import autoclass

        DecryptionClass = autoclass('exem.ae.analysis.utils.Decryption')
        self.decryption_class = DecryptionClass()

    def execute_bind_list_decoding(self, enc):
        try:
            decrypt = self.decryption_class.getDecToEncBindValue(enc)

        except Exception as e:
            decrypt = enc

        finally:
            js=DecodingJs()
            return js.convert_bind(decrypt)



