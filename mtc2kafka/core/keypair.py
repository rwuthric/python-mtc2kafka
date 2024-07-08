import rsa
import os


def generateKeyPair(privateKeyFile, publicKeyFile):
    """
    Generates and saves a public/private key pair
    File access for Private Key is set to '-rw-------'
    """
    publicKey, privateKey = rsa.newkeys(512)
    keydata = publicKey.save_pkcs1()
    f = open(publicKeyFile, "wb")
    f.write(keydata)
    f.close()

    keydata = privateKey.save_pkcs1()
    f = open(privateKeyFile, "wb")
    f.write(keydata)
    f.close()
    # Private Key : only owner has rw access
    os.chmod(privateKeyFile, 0o600)
