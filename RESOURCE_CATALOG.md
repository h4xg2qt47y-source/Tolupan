# Tol (Tolupan/Jicaque) Language Resource Catalog

**Language**: Tol (also: Tolupan, Jicaque, Xicaque, Tolpan)  
**ISO 639-3**: jic  
**Region**: Montaña de la Flor, Francisco Morazán & Yoro, Honduras  
**Speakers**: ~200-600 remaining  
**Status**: Severely Endangered  

---

## 1. AUDIO FILES WITH TRANSCRIPTS (Tol Audio/)

### 1a. ScriptureEarth New Testament Audio (~1 GB total)
- **Location**: `Tol Audio/ScriptureEarth_NT_Audio/`
- **Format**: MP3 files, one per chapter
- **Content**: Complete Tol New Testament read aloud
- **Naming**: `{book_number}-{book_code}jic-{chapter}.mp3`
- **Paired text**: `Tol Translation/Tol_NT_Complete.pdf` and `Tol Translation/NT_Text/` (317 HTML chapter files)
- **Source**: https://scriptureearth.org/data/jic/audio/
- **Value for ML**: ~20+ hours of spoken Tol with exact word-for-word text alignment possible

### 1b. ELAN Transcripts with Tol-Spanish Parallel Text
- **Location**: `Tol Audio/ELAN_Transcripts/`
- **Files**:
  - `Bees_Tol_Spanish.html` - Conversation about bees/honey gathering
  - `Ghost_Stories_Tol_Spanish.html` - Ghost story narratives
  - `Elicited_Grammar_Aurelio.html` - Elicited vocabulary & verb conjugations
  - `tol_spanish_parallel.tsv` - **752 extracted Tol↔Spanish parallel entries**
  - `tol_spanish_parallel.json` - Same data in JSON format
- **Source**: https://languageconservation.org/index.php/projects/tol
- **Value for ML**: Direct sentence-level Tol-Spanish parallel data with timestamps

### 1c. ELAR Archive (Requires Free Registration)
- **URL**: https://www.elararchive.org/dk0118/
- **Content**: 25+ recordings including:
  - Sociolinguistic interviews (ET-1 through ET-31)
  - Narratives (Ghost stories, Let's Play House, Basketmaking)
  - Elicitation sessions (SE-01 through SE-09)
  - National Hymn in Tol
  - Final survey report
- **Depositor**: Steffen Haurholm-Larsen, University of Zurich
- **Access**: Register free at elararchive.org → download audio/video + transcripts
- **Action needed**: Manual browser download after free registration

### 1d. Smithsonian National Anthropological Archives
- **Collection**: Anne Chapman papers on the Tolupan (NAA.1996-15)
- **Content**: 30 sound recordings with transcripts (1955-1994)
- **URL**: https://sova.si.edu/record/naa.1996-15
- **Contact**: naa@si.edu | 301-238-1310
- **Action needed**: Contact Smithsonian for digital access to recordings

### 1e. ScriptureEarth Videos (Tol with Spanish subtitles)
- **URL**: https://www.scriptureearth.org/00spa.php?idx=69&iso=jic
- **Content**: 13 general Scripture videos + 42 Gospel of John videos in Tol
- **Action needed**: Download via browser (ZIP download available on site)

---

## 2. TRANSLATION RESOURCES (Tol Translation/)

### 2a. Downloaded Resources
| File | Description | Size | Source |
|------|-------------|------|--------|
| `DiccTol_Jicaque_Espanol_Dennis_1983.pdf` | **Tol-Spanish / Spanish-Tol Dictionary** (Dennis & Dennis, 1983, 139 pages) | 8.2 MB | Local + SIL |
| `English_Tol_Dictionary_Dennis_1983.pdf` | **English–Tol** companion (same headwords; English glosses via Argos es→en from OCR) | — | Generated (`scripts/build_en_tol_dictionary.py`) |
| `Tol_NT_Wycliffe_803p.pdf` | Tol New Testament, Wycliffe edition (803 pages, more detailed) | 8.1 MB | Local |
| `Tol_NT_Complete.pdf` | Tol New Testament, eBible edition (483 pages, text-extractable) | 3.9 MB | eBible.org |
| `Tol_NT_Marcos.pdf` | Gospel of Mark in Tol | 393 KB | eBible.org |
| `Tol_NT_Juan.pdf` | Gospel of John in Tol | 472 KB | eBible.org |
| `Tol_NT.epub` | NT in ePub format (structured, machine-readable) | 1.1 MB | eBible.org |
| `Tol_NT_text.zip` | NT as 317 HTML chapter files | 1.3 MB | eBible.org |
| `NT_Text/` | Extracted HTML chapter files | ~317 files | eBible.org |

### 2b. Still Needed (Browser Download - Cloudflare Protected)
| Resource | URL | Size |
|----------|-----|------|
| **Tol School Dictionary** (Dennis & Dennis, 2001, 156 pages) | https://www.sil.org/resources/archives/26268 | 5.86 MB |

**Instructions**: Open the URL in a web browser to download (SIL has Cloudflare protection that blocks automated downloads).

### 2c. Online Bible Reading
- **Bible.is**: https://www.bible.com (search "JICNT" or "Tol")
- **eBible.org**: https://ebible.org/study/?w1=bible&t1=local%3AjicNT&v1=JN1_1
- **YouVersion**: Search "Dios Tjevele Jupj ꞌÜsüs La Qjuisiji Jesucristo Mpes"

### 2d. Parallel Text Data
- **ELAN Transcripts**: 752 Tol-Spanish parallel sentence pairs (see `Tol Audio/ELAN_Transcripts/`)
- **NT Text + Audio**: The NT text (317 chapters) paired with audio creates a massive aligned corpus

---

## 3. PRONUNCIATION & LINGUISTIC RESOURCES (Tol Pronunciation/)

### 3a. Downloaded Resources
| File | Description | Size | Source |
|------|-------------|------|--------|
| `Tol_Jicaque_Grammar_Holt.pdf` | **Tol grammar description** (Holt, 33 pages) | 15 MB | Local |
| `Haurholm-Larsen_GrammaticalCategories_Slides.pdf` | Grammatical categories in Tol, MPI presentation (46 slides) | 6.2 MB | Local |
| `Jicaque_Torrupan_Indians_VonHagen_1943.pdf` | Ethnographic/linguistic study (Von Hagen, 1943, 132 pages) | 5.8 MB | Local |
| `Tol_Jicaque_Language_Overview_41p.pdf` | Comprehensive language overview (41 pages, likely Haurholm-Larsen) | 614 KB | Local |
| `El_Alfabeto_Tol_1975.pdf` | Tol alphabet/orthography (Dennis, Dennis & Fleming, 1975) | 600 KB | SIL/IHAH |
| `Jicaque_Hokan_Classification_1953.pdf` | Jicaque as a Hokan language (1953, 8 pages) | 396 KB | Local |
| `Tol_Language_Cozemius_1923.pdf` | Early Tol documentation (Cozemius, 1923, 8 pages) | 224 KB | Local |
| `Tol_Jicaque_Wikipedia.html` | Wikipedia article with phonological inventory | 117 KB | Web |

### 3b. Resources Still Requiring Browser/Purchase
| Resource | URL | Notes |
|----------|-----|-------|
| **Tol (Jicaque) Phonology** (Fleming & Dennis, 1977) | https://www.jstor.org/stable/1264929 | Free JSTOR account required |
| **Proto-Jicaque Comparative Reconstruction** (Oltrogge, 1977) | https://www.sil.org/resources/archives/8771 | Browser download (Cloudflare) |
| **Sociolinguistic survey** (Haurholm-Larsen) | https://www.academia.edu/1818616/ | Free Academia.edu account |
| **Proto-Tol (Jicaque)** reconstruction | https://www.academia.edu/10153282/Proto_Tol_Jicaque_ | Free Academia.edu account |

### 3c. Tol Consonant Inventory (from Wikipedia)
Bilabial, Alveolar, Palatal, Velar, Glottal stops and fricatives.
Key phonemes include: /p, t, k, ʔ, ts, tʃ, s, ʃ, h, m, n, l, w, j/
Vowels: /i, e, a, o, u/ (with length distinctions)

---

## 4. ALL KNOWN SOURCE URLs

### Archives & Databases
- ELAR Archive: https://www.elararchive.org/dk0118/
- OLAC Language Archive: http://www.language-archives.org/language/jic
- Endangered Languages Project: https://endangeredlanguages.com/lang/2055
- Glottolog: https://glottolog.org/resource/languoid/id/jica1245
- Smithsonian (Chapman papers): https://sova.si.edu/record/naa.1996-15

### SIL Resources
- Dictionary 1983: https://www.sil.org/resources/archives/26210
- School Dictionary 2001: https://www.sil.org/resources/archives/26268
- Tol Alphabet 1975: https://sil.org/resources/archives/5058
- Phonology 1977: https://www.sil.org/resources/archives/3492

### Bible/Scripture
- ScriptureEarth: https://www.scriptureearth.org/00i-Scripture_Index.php?iso=jic
- eBible.org: https://ebible.org/find/details.php?id=jicNT
- Bible.is: https://www.bible.is
- Joshua Project: https://joshuaproject.net/languages/jic

### Academic
- Language Conservation Project: https://languageconservation.org/index.php/projects/tol
- Culture in Crisis: https://cultureincrisis.org/projects/survey-description-and-documentation-of-tol-jicaque-of-honduras

---

## 5. RECOMMENDATIONS FOR BUILDING TOL AUDIO TRANSLATION SYSTEM

See detailed recommendations in the main conversation or below.
