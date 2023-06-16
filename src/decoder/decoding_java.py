import jnius_config
from src.decoder.decoding import Decoding

java_resource_jar_path = "C:/Users/exem/Downloads/test/intermax_decryption.jar"
jnius_config.add_options('-Xms128m', '-Xmx256m')
jnius_config.set_classpath(java_resource_jar_path)

from jnius import autoclass

DecryptionClass = autoclass('exem.ae.analysis.utils.Decryption')
decryption_class = DecryptionClass()

def excute_decoding_java(enc):

    try:
        encrypted = decryption_class.getDecToEncBindValue(enc)

    except Exception as e:
        encrypted = enc
        pass

    finally:
        Decoding.convertBindList(encrypted)

    return Decoding.convertBindList(encrypted)


