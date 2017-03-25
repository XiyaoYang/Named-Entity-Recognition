# Named-Entity-Recognition

Clinical Note Semantic Indexing 

MetaMap is a highly configurable program developed by Dr. Alan (Lan) Aronson at the National Library of Medicine (NLM) to map biomedical text to the UMLS Metathesaurus or, equivalently, to discover Metathesaurus concepts referred to in text. MetaMap uses a knowledge-intensive approach based on symbolic, natural-language processing (NLP) and computational-linguistic techniques.

The Metathesaurus forms the base of the UMLS and comprises over 1 million biomedical concepts and 5 million concept names, all of which stem from the over 100 incorporated controlled vocabularies and classification systems. Some examples of the incorporated controlled vocabularies are ICD-10, MeSH, SNOMED CT, DSM-IV, LOINC, etc.

This simple Python script acts as a concept extractor in the clinical data natural language processing workflow. It is developed based on MetaMap API. Other parts of the workflow consist a sentence chunker and an assertation tool, which will finally output:

• the concept’s confidence score (MetaMap Algorithm),

• the UMLS string matched,

• the concept’s Preferred Name, and 

• the concept’s Semantic Type(s)
