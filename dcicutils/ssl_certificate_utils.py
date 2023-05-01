from cryptography.hazmat.primitives import serialization
from datetime import datetime
import OpenSSL
import socket
import ssl
from typing import Optional, Tuple
from .misc_utils import future_datetime


def get_ssl_certificate_info(hostname: str,
                             raise_exception: bool = False,
                             test_mode_certificate_expiration_warning_days: int = 0) -> Optional[dict]:
    """
    Returns a dictionary containing various data points for the SSL certificate of the given
    hostname. If an error is encountered then returns None, or, if the given raise_exception
    argument is True then raises an exception. Returned dictionary contains (alphabetical order):

        active_at    issuer           owner_country
        exception    issuer_city      owner_entity
        expired      issuer_country   owner_state
        expires_at   issuer_entity    pem
        hostname     issuer_state     public_key_pem
        hostnames    owner            serial_number
        inactive     owner_city       invalid
    """
    hostname = _normalize_hostname(hostname)
    try:
        certificate_pem = _get_ssl_certificate_pem(hostname, raise_exception=False)
        certificate_info = _get_ssl_certificate_info_from_pem(
            certificate_pem,
            raise_exception=raise_exception,
            expires_soon_days=test_mode_certificate_expiration_warning_days
        )
        certificate_okay, certificate_exception = _is_ssl_certificate_okay(hostname, raise_exception=raise_exception)
        # The hostname from _get_ssl_certificate_info_from_pem is not necessarily exactly correct;
        # for example, for cgap-wolf.hms.harvard.edu it is imperva.com.
        certificate_info["hostname"] = hostname
        certificate_info["invalid"] = certificate_info["invalid"] or not certificate_okay
        if certificate_exception:
            certificate_info["exception"] = str(certificate_exception)
        return certificate_info
    except Exception as e:
        if raise_exception:
            raise e
        return None


_SSL_PORT = 443
_SSL_CERTIFICATE_EXPIRES_SOON_WARNING_DAYS = 7


def _get_ssl_certificate_pem(hostname: str, raise_exception: bool = False) -> Optional[str]:
    """
    Returns the SSL certificate as a PEM string for the given hostname. If an error is encountered
    then returns None, or, if the given raise_exception argument is True then raises an exception.
    """
    try:
        with ssl.create_connection((hostname, _SSL_PORT)) as socket_connection:
            # Note that we turn off certificate verification for getting the PEM here,
            # because we want the PEM regardless of whether or not the certificate is
            # valid; we do the real certificate sanity check in _is_ssl_certificate_okay.
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            with context.wrap_socket(socket_connection, server_hostname=hostname) as socket_context:
                certificate = socket_context.getpeercert(binary_form=True)
                return ssl.DER_cert_to_PEM_cert(certificate)
    except Exception as e:
        if raise_exception:
            raise e
        return None


def _get_ssl_certificate_info_from_pem(pem_string: str,
                                       raise_exception: bool = False,
                                       expires_soon_days: int = 0) -> Optional[dict]:
    """
    Returns a dictionary containing various data points for the given SSL certificate string
    string in PEM format. If an error is encountered in parsing this given string then returns
    None by default, or raises and exception of the given raise_exception argument is True.
    """
    now = datetime.now()
    if expires_soon_days <= 0:
        expires_soon_days = _SSL_CERTIFICATE_EXPIRES_SOON_WARNING_DAYS

    def get_hostnames(certificate: OpenSSL.crypto.X509) -> list:
        """
        Returns the list of hostnames associated with the given SSL certificate. There can indeed
        be more than one hostname associated with a certificate, and we cannot determine the one
        primary one associated with it, so the caller (get_ssl_certificate_info) of this (outer)
        function (_get_ssl_certificate_info_from_pem) will set the hostname in the returned dictionary.
        """
        sans = []
        extension_count = certificate.get_extension_count()
        for i in range(extension_count):
            extension = certificate.get_extension(i)
            if extension.get_short_name() == b"subjectAltName":
                sans_list = str(extension).replace(" ", "").split(",")
                for san in sans_list:
                    if san.startswith("DNS:"):
                        sans.append(san[4:])
        return sorted(sans)

    try:
        certificate = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, pem_string)

        subject = certificate.get_subject()
        common_name = subject.commonName
        hostnames = get_hostnames(certificate)
        owner_country = subject.countryName
        owner_state = subject.stateOrProvinceName
        owner_city = subject.localityName
        owner_entity = subject.organizationName
        owner = subject.organizationalUnitName or owner_entity
        if not owner:
            owner = common_name

        issuer = certificate.get_issuer()
        issuer_country = issuer.countryName
        issuer_state = issuer.stateOrProvinceName
        issuer_city = issuer.localityName
        issuer_entity = issuer.organizationName
        issuer = issuer.organizationalUnitName or issuer_entity

        not_before = certificate.get_notBefore().decode("UTF-8")
        not_after = certificate.get_notAfter().decode("UTF-8")
        active_at = datetime.strptime(not_before, "%Y%m%d%H%M%SZ")
        expires_at = datetime.strptime(not_after, "%Y%m%d%H%M%SZ")
        expires_soon = expires_at <= future_datetime(now=now, days=expires_soon_days)

        expired = now >= expires_at
        inactive = now <= active_at
        invalid = inactive or expired

        # Note we could not currently get detect specifically if the certificate is revoked;
        # could not get the recommeneded code working consistently; no great matter.

        serial_number = str(certificate.get_serial_number())
        public_key_pem = certificate.get_pubkey().to_cryptography_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("UTF-8")

        return {
            "name": None,  # Filled in by caller (get_ssl_certificate_info)
            "hostname": None,  # Filled in by caller (get_ssl_certificate_info)
            "hostnames": hostnames,
            "common_name": common_name,
            "owner": owner,
            "owner_entity": owner_entity,
            "owner_country": owner_country,
            "owner_state": owner_state,
            "owner_city": owner_city,
            "issuer": issuer,
            "issuer_entity": issuer_entity,
            "issuer_country": issuer_country,
            "issuer_state": issuer_state,
            "issuer_city": issuer_city,
            "active_at": active_at.strftime("%Y-%m-%d %H:%M:%S"),
            "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S"),
            "pem": pem_string,
            "serial_number": serial_number,
            "public_key_pem": public_key_pem,
            "invalid": invalid,
            "inactive": inactive,
            "expired": expired,
            "expires_soon": expires_soon,
            "exception": None,
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
        }

    except Exception as e:
        if raise_exception:
            raise e
        return None


def _is_ssl_certificate_okay(hostname: str, raise_exception: bool = False) -> Tuple[bool, Optional[Exception]]:
    """
    Tries referencing the SSL certificate (in a couple different ways) of the given hostname
    so as to verify that the certificate is good, and if so, then returns a tuple containing,
    in order, True and None. If an error (exception) occurs then returns a tuple containing,
    in order, False and the Exception which was raised, or, if the given raise_exception
    argument is True then simply raises an exception.
    """
    try:
        # Note that with Python versions below 3.10 this fails on some sites (e.g. slashdot.org):
        # - ssl.get_server_certificate((hostname, _SSL_PORT))
        # https://stackoverflow.com/questions/70994539/ssl-errors-for-ssl-get-server-certificate-on-some-websites-but-not-on-others
        # FYI sample certificates: https://www.ssl.com/sample-valid-revoked-and-expired-ssl-tls-certificates/
        with socket.create_connection((hostname, _SSL_PORT)) as socket_connection:
            with ssl.create_default_context().wrap_socket(socket_connection, server_hostname=hostname):
                return True, None
    except Exception as e:
        if raise_exception:
            raise e
        return False, e


def _normalize_hostname(hostname: str) -> str:
    """
    If the given hostname actually looks like an URL then return just the hostname thereof,
    otherwise just return the given hostname; also lower-cases the value.
    """
    if hostname:
        hostname = hostname.lower()
        if hostname.startswith("https://"):
            hostname = hostname[8:]
        elif hostname.startswith("http://"):
            hostname = hostname[7:]
        slash = hostname.find("/")
        if slash > 0:
            hostname = hostname[:slash]
    return hostname
