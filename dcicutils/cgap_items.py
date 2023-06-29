from __future__ import annotations
import math
from dataclasses import dataclass
from typing import List, Union

import structlog

from .item_models import JsonObject, PortalItem, SubembeddedProperty

logger = structlog.getLogger(__name__)


@dataclass(frozen=True)
class CommonPropertiesMixin:
    ACCESSION = "accession"
    ALIASES = "aliases"
    DATE_MODIFIED = "date_modified"
    DISPLAY_TITLE = "display_title"
    INSTITUTION = "institution"
    LAST_MODIFIED = "last_modified"
    MODIFIED_BY = "modified_by"
    PROJECT = "project"
    SCHEMA_VERSION = "schema_version"
    STATUS = "status"
    TAGS = "tags"

    @property
    def accession(self) -> str:
        return self._properties.get(self.ACCESSION, "")

    @property
    def display_title(self) -> str:
        return self._properties.get(self.DISPLAY_TITLE, "")

    @property
    def schema_version(self) -> str:
        return self._properties.get(self.SCHEMA_VERSION, "")

    @property
    def aliases(self) -> List[str]:
        return self._properties.get(self.ALIASES, [])

    @property
    def status(self) -> str:
        return self._properties.get(self.STATUS, "")

    @property
    def date_created(self) -> str:
        return self._properties.get(self.DATE_CREATED, "")

    @property
    def last_modified(self) -> JsonObject:
        return self._properties.get(self.LAST_MODIFIED, {})

    @property
    def date_modified(self) -> str:
        return self.last_modified.get(self.DATE_MODIFIED, "")

    @property
    def modified_by(self) -> Union[User, str]:
        property_value = self.last_modified.get(self.MODIFIED_BY, "")
        return self._get_embeddable_property(property_value, User)

    @property
    def tags(self) -> List[str]:
        return self._properties.get(self.TAGS, [])

    @property
    def project(self) -> Union[Project, str]:
        property_value = self._properties.get(self.PROJECT, "")
        return self._get_embeddable_property(property_value, Project)

    @property
    def institution(self) -> Union[Institution, str]:
        property_value = self._properties.get(self.INSTITUTION, "")
        return self._get_embeddable_property(property_value, Institution)


@dataclass(frozen=True)
class CGAPItem(PortalItem, CommonPropertiesMixin):
    pass


@dataclass(frozen=True)
class Project(CGAPItem):
    NAME = "name"
    TITLE = "title"

    @property
    def name(self) -> str:
        return self._properties.get(self.NAME, "")

    @property
    def title(self) -> str:
        return self._properties.get(self.TITLE, "")


@dataclass(frozen=True)
class Institution(CGAPItem):
    NAME = "name"
    TITLE = "title"

    @property
    def name(self) -> str:
        return self._properties.get(self.NAME, "")

    @property
    def title(self) -> str:
        return self._properties.get(self.TITLE, "")


@dataclass(frozen=True)
class User(CGAPItem):
    FIRST_NAME = "first_name"
    INSTITUTION = "institution"
    LAST_NAME = "last_name"
    PROJECT = "project"
    USER_INSTITUTION = "user_institution"

    @property
    def first_name(self) -> str:
        return self._properties.get(self.FIRST_NAME, "")

    @property
    def last_name(self) -> str:
        return self._properties.get(self.LAST_NAME, "")

    @property
    def institution(self) -> Union[str, JsonObject]:
        property_value = self._properties.get(self.INSTITUTION, "")
        return self._get_embeddable_property(property_value, Institution)

    @property
    def project(self) -> Union[str, JsonObject]:
        property_value = self._properties.get(self.PROJECT, "")
        return self._get_embeddable_property(property_value, Project)

    @property
    def user_institution(self) -> Union[str, JsonObject]:
        property_value = self._properties.get(self.USER_INSTITUTION, "")
        return self._get_embeddable_property(property_value, Institution)


@dataclass(frozen=True)
class FileFormat(CGAPItem):
    @property
    def file_format(self) -> str:
        return self._properties.get(self.FILE_FORMAT, "")


@dataclass(frozen=True)
class File(CGAPItem):
    @property
    def file_format(self):
        property_value = self._properties.get(self.FILE_FORMAT, "")
        return self._get_embeddable_property(property_value, FileFormat)


@dataclass(frozen=True)
class VariantConsequence(CGAPItem):
    # Schema constants
    IMPACT = "impact"
    IMPACT_HIGH = "HIGH"
    IMPACT_LOW = "LOW"
    IMPACT_MODERATE = "MODERATE"
    IMPACT_MODIFIER = "MODIFIER"
    VAR_CONSEQ_NAME = "var_conseq_name"

    DOWNSTREAM_GENE_CONSEQUENCE = "downstream_gene_variant"
    FIVE_PRIME_UTR_CONSEQUENCE = "5_prime_UTR_variant"
    THREE_PRIME_UTR_CONSEQUENCE = "3_prime_UTR_variant"
    UPSTREAM_GENE_CONSEQUENCE = "upstream_gene_variant"

    @property
    def impact(self) -> str:
        return self._properties.get(self.IMPACT, "")

    @property
    def name(self) -> str:
        return self._properties.get(self.VAR_CONSEQ_NAME, "")

    def get_name(self) -> str:
        return self.name

    def get_impact(self) -> str:
        return self.impact

    def is_downstream(self) -> str:
        return self.name == self.DOWNSTREAM_GENE_CONSEQUENCE

    def is_upstream(self) -> str:
        return self.name == self.UPSTREAM_GENE_CONSEQUENCE

    def is_three_prime_utr(self) -> str:
        return self.name == self.THREE_PRIME_UTR_CONSEQUENCE

    def is_five_prime_utr(self) -> str:
        return self.name == self.FIVE_PRIME_UTR_CONSEQUENCE


@dataclass(frozen=True)
class Transcript(SubembeddedProperty):
    # Schema constants
    CSQ_CANONICAL = "csq_canonical"
    CSQ_CONSEQUENCE = "csq_consequence"
    CSQ_DISTANCE = "csq_distance"
    CSQ_EXON = "csq_exon"
    CSQ_FEATURE = "csq_feature"
    CSQ_INTRON = "csq_intron"
    CSQ_MOST_SEVERE = "csq_most_severe"

    # Class constants
    LOCATION_EXON = "Exon"
    LOCATION_INTRON = "Intron"
    LOCATION_DOWNSTREAM = "bp downstream"
    LOCATION_UPSTREAM = "bp upstream"
    LOCATION_FIVE_PRIME_UTR = "5' UTR"
    LOCATION_THREE_PRIME_UTR = "3' UTR"
    IMPACT_RANKING = {
        VariantConsequence.IMPACT_HIGH: 0,
        VariantConsequence.IMPACT_MODERATE: 1,
        VariantConsequence.IMPACT_LOW: 2,
        VariantConsequence.IMPACT_MODIFIER: 3,
    }

    @property
    def canonical(self) -> bool:
        return self._properties.get(self.CSQ_CANONICAL, False)

    @property
    def most_severe(self) -> bool:
        return self._properties.get(self.CSQ_MOST_SEVERE, False)

    @property
    def exon(self) -> str:
        return self._properties.get(self.CSQ_EXON, "")

    @property
    def intron(self) -> str:
        return self._properties.get(self.CSQ_INTRON, "")

    @property
    def distance(self) -> str:
        return self._properties.get(self.CSQ_DISTANCE, "")

    @property
    def feature(self) -> str:
        return self._properties.get(self.CSQ_FEATURE, "")

    @property
    def consequences(self) -> List[Union[VariantConsequence, str]]:
        consequences = self._properties.get(self.CSQ_CONSEQUENCE, [])
        return [
            self._get_embeddedable_property(consequence, VariantConsequence)
            for consequence in consequences
        ]

    def _are_consequences_embedded(self) -> bool:
        return [
            isinstance(consequence, VariantConsequence)
            for consequence in self.consequences
        ]

    def is_canonical(self) -> bool:
        return self.canonical

    def is_most_severe(self) -> bool:
        return self.most_severe

    def get_feature(self) -> str:
        return self.feature

    def get_location(self) -> str:
        if not self._are_consequences_embedded():
            raise ValueError(f"Consequence calculations not possible")
        result = ""
        most_severe_consequence = self._get_most_severe_consequence()
        if most_severe_consequence:
            result = self._get_location(most_severe_consequence)
        return result

    def _get_most_severe_consequence(self) -> Union[VariantConsequence, None]:
        result = None
        most_severe_rank = math.inf
        for consequence in self.consequences:
            impact = consequence.get_impact()
            impact_rank = self.IMPACT_RANKING.get(impact, math.inf)
            if impact_rank < most_severe_rank:
                result = consequence
        return result

    def _get_location(self, most_severe_consequence: VariantConsequence) -> str:
        result = ""
        if self.exon:
            result = self._get_exon_location()
        elif self.intron:
            result = self._get_intron_location()
        elif self.distance:
            result = self._get_distance_location(most_severe_consequence)
        return self._add_utr_suffix_if_needed(result, most_severe_consequence)

    def _get_exon_location(self) -> str:
        return f"{self.LOCATION_EXON} {self.exon}"

    def _get_intron_location(self) -> str:
        return f"{self.LOCATION_INTRON} {self.intron}"

    def _get_distance_location(self, consequence: VariantConsequence) -> str:
        if consequence.is_upstream():
            return f"{self.distance} {self.LOCATION_UPSTREAM}"
        if consequence.is_downstream():
            return f"{self.distance} {self.LOCATION_DOWNSTREAM}"
        return ""

    def _add_utr_suffix_if_needed(
        self, location: str, consequence: VariantConsequence
    ) -> str:
        if consequence.is_three_prime_utr():
            return self._add_three_prime_utr_suffix(location)
        if consequence.is_five_prime_utr():
            return self._add_five_prime_utr_suffix(location)
        return location

    def _add_three_prime_utr_suffix(self, location: str) -> str:
        return self._add_utr_suffix(location, self.LOCATION_THREE_PRIME_UTR)

    def _add_five_prime_utr_suffix(self, location: str) -> str:
        return self._add_utr_suffix(location, self.LOCATION_FIVE_PRIME_UTR)

    def _add_utr_suffix(self, location: str, utr_suffix: str) -> str:
        if location:
            return f"{location} ({utr_suffix})"
        return utr_suffix

    def get_consequence_names(self) -> str:
        if not self._are_consequences_embedded():
            raise ValueError(f"Consequence calculations not possible")
        return ", ".join([consequence.get_name() for consequence in self.consequences])


@dataclass(frozen=True)
class Variant(CGAPItem):
    # Schema constants
    CSQ_CANONICAL = "csq_canonical"
    CSQ_CONSEQUENCE = "csq_consquence"
    CSQ_FEATURE = "csq_feature"
    CSQ_GNOMADE2_AF_POPMAX = "csq_gnomade2_af_popmax"
    CSQ_GNOMADG_AF_POPMAX = "csq_gnomadg_af_popmax"
    CSQ_MOST_SEVERE = "csq_most_severe"
    DISTANCE = "distance"
    EXON = "exon"
    INTRON = "intron"
    MOST_SEVERE_LOCATION = "most_severe_location"
    TRANSCRIPT = "transcript"

    GNOMAD_V2_AF_PREFIX = "csq_gnomade2_af-"
    GNOMAD_V3_AF_PREFIX = "csq_gnomadg_af-"
    GNOMAD_POPULATION_SUFFIX_TO_NAME = {
        "afr": "African-American/African",
        "ami": "Amish",
        "amr": "Latino",
        "asj": "Ashkenazi Jewish",
        "eas": "East Asian",
        "fin": "Finnish",
        "mid": "Middle Eastern",
        "nfe": "Non-Finnish European",
        "oth": "Other Ancestry",
        "sas": "South Asian",
    }

    @property
    def transcript(self) -> List[JsonObject]:
        return self._properties.get(self.TRANSCRIPT, [])

    @property
    def _transcripts(self) -> List[Transcript]:
        return [
            Transcript.from_properties(transcript, self)
            for transcript in self.transcript
        ]

    @property
    def most_severe_location(self) -> str:
        return self._properties.get(self.MOST_SEVERE_LOCATION, "")

    @property
    def csq_gnomadg_af_popmax(self) -> Union[float, None]:
        return self._properties.get(self.CSQ_GNOMADG_AF_POPMAX)

    @property
    def csq_gnomade2_af_popmax(self) -> Union[float, None]:
        return self._properties.get(self.CSQ_GNOMADE2_AF_POPMAX)

    @property
    def _canonical_transcript(self) -> Union[None, Transcript]:
        for transcript in self._transcripts:
            if transcript.is_canonical():
                return transcript

    @property
    def _most_severe_transcript(self) -> Union[None, Transcript]:
        for transcript in self._transcripts:
            if transcript.is_most_severe():
                return transcript

    def get_most_severe_location(self) -> str:
        return self.most_severe_location

    def get_canonical_transcript_feature(self) -> str:
        if self._canonical_transcript:
            return self._canonical_transcript.get_feature()
        return ""

    def get_most_severe_transcript_feature(self) -> str:
        if self._most_severe_transcript:
            return self._most_severe_transcript.get_feature()
        return ""

    def get_canonical_transcript_consequence_names(self) -> str:
        if self._canonical_transcript:
            return self._canonical_transcript.get_consequence_names()
        return ""

    def get_most_severe_transcript_consequence_names(self) -> str:
        if self._most_severe_transcript:
            return self._most_severe_transcript.get_consequence_names()
        return ""

    def get_canonical_transcript_location(self) -> str:
        if self._canonical_transcript:
            return self._canonical_transcript.get_location()
        return ""

    def get_most_severe_transcript_location(self) -> str:
        if self._most_severe_transcript:
            return self._most_severe_transcript.get_location()
        return ""

    def get_gnomad_v3_popmax_population(self) -> str:
        result = ""
        gnomad_v3_af_popmax = self.csq_gnomadg_af_popmax
        if gnomad_v3_af_popmax:
            result = self._get_gnomad_v3_population_for_allele_fraction(
                gnomad_v3_af_popmax
            )
        return result

    def get_gnomad_v2_popmax_population(self) -> str:
        result = ""
        gnomad_v2_af_popmax = self.csq_gnomade2_af_popmax
        if gnomad_v2_af_popmax:
            result = self._get_gnomad_v2_population_for_allele_fraction(
                gnomad_v2_af_popmax
            )
        return result

    def _get_gnomad_v3_population_for_allele_fraction(
        self, allele_fraction: float
    ) -> str:
        return self._get_gnomad_population_for_allele_fraction(
            self.GNOMAD_V3_AF_PREFIX, allele_fraction
        )

    def _get_gnomad_v2_population_for_allele_fraction(
        self, allele_fraction: float
    ) -> str:
        return self._get_gnomad_population_for_allele_fraction(
            self.GNOMAD_V2_AF_PREFIX, allele_fraction
        )

    def _get_gnomad_population_for_allele_fraction(
        self, gnomad_af_prefix: str, allele_fraction: float
    ) -> str:
        result = ""
        for (
            gnomad_suffix,
            population_name,
        ) in self.GNOMAD_POPULATION_SUFFIX_TO_NAME.items():
            population_property_name = gnomad_af_prefix + gnomad_suffix
            allele_frequency = self._properties.get(population_property_name)
            if allele_frequency == allele_fraction:
                result = population_name
                break
        return result


@dataclass(frozen=True)
class Note(CGAPItem):
    pass


@dataclass(frozen=True)
class VariantSample(CGAPItem):
    # Schema constants
    VARIANT = "variant"

    @property
    def variant(self) -> Variant:
        return Variant(self._properties.get(self.VARIANT, {}))

    def get_canonical_transcript_feature(self) -> str:
        return self.variant.get_canonical_transcript_feature()

    def get_canonical_transcript_location(self) -> str:
        return self.variant.get_canonical_transcript_location()

    def get_canonical_transcript_consequence_names(self) -> str:
        return self.variant.get_canonical_transcript_consequence_names()

    def get_most_severe_transcript_feature(self) -> str:
        return self.variant.get_most_severe_transcript_feature()

    def get_most_severe_transcript_location(self) -> str:
        return self.variant.get_most_severe_transcript_location()

    def get_most_severe_transcript_consequence_names(self) -> str:
        return self.variant.get_most_severe_transcript_consequence_names()

    def get_gnomad_v3_popmax_population(self) -> str:
        return self.variant.get_gnomad_v3_popmax_population()

    def get_gnomad_v2_popmax_population(self) -> str:
        return self.variant.get_gnomad_v2_popmax_population()


@dataclass(frozen=True)
class VariantSampleSelectionFromVariantSampleList(SubembeddedProperty):
    VARIANT_SAMPLE_ITEM = "variant_sample_item"

    @property
    def variant_sample_item(self) -> Union[VariantSample, str]:
        variant_sample = self._properties.get(self.VARIANT_SAMPLE_ITEM, "")
        return self._get_embeddable_property(variant_sample, VariantSample)

    def get_variant_sample(self):
        return self.variant_sample_item


@dataclass(frozen=True)
class VariantSampleList(CGAPItem):
    CREATED_FOR_CASE = "created_for_case"
    VARIANT_SAMPLES = "variant_samples"

    @property
    def created_for_case(self) -> str:
        return self._properties.get(self.CREATED_FOR_CASE, "")

    @property
    def variant_samples(self) -> List[VariantSampleSelectionFromVariantSampleList]:
        variant_samples = self._properties.get(self.VARIANT_SAMPLES, [])
        return [
            VariantSampleSelectionFromVariantSampleList.from_properties(
                variant_sample, parent_item=self
            )
            for variant_sample in variant_samples
        ]

    def get_associated_case_accession(self) -> str:
        return self.created_for_case

    def get_variant_samples(self) -> List[Union[VariantSample, str]]:
        return [
            variant_sample_selection.get_variant_sample()
            for variant_sample_selection in self.variant_samples
        ]
