const fs = require('fs')
const async = require('async')
const xml2js = require('xml2js')
const moment = require('moment')
require('../log')('BODACC import')
const path = require('path')
const endOfLine = require('os').EOL
const csv = require('csv/sync')

moment.locale('fr')

const xml2jsOpts = {
  explicitArray: false,
  valueProcessors: [(val) => {
    val = val.trim()
    val = val.replace(/\s{2,}/g, ' ')
    return val
  }]
}

const rootElements = {
  'RCS-A': 'RCS-A_IMMAT',
  'RCS-B': 'RCS-B_REDIFF',
  BILAN: 'Bilan_XML_Rediff',
  PCL: 'PCL_REDIFF'
}

function fixKeys (obj, srcKeys, destKey) {
  if (!obj) return
  srcKeys.forEach(srcKey => {
    if (obj[srcKey] !== undefined) {
      obj[destKey] = obj[srcKey]
      delete obj[srcKey]
    }
  })
}

function moveKeys (src, dest, keys) {
  keys.filter(key => src[key] !== undefined).forEach(key => {
    dest[key] = src[key]
    delete src[key]
  })
}

function toArray (obj, key) {
  if (!obj || obj[key] === undefined) return
  if (Array.isArray(obj[key])) return
  obj[key] = [obj[key]]
  obj[key] = obj[key].filter(val => val)
}

function assertEmpty (obj) {
  if (Object.keys(obj).length > 0) throw new Error('Object should be empty after being fully used. Remaining keys: ' + JSON.stringify(Object.keys(obj)))
}

function fixAdresse (adresse) {
  if (Object.keys(adresse).length === 0) return
  if (Object.keys(adresse).length > 1) console.error('Multiple adresses.') // ,adresse
  const pays = Object.keys(adresse)[0].toUpperCase()
  Object.assign(adresse, { pays }, adresse[pays])
  delete adresse[pays]
  return adresse
}

function fixPersonne (personne) {
  if (personne.personneMorale) {
    Object.assign(personne, { type: 'personneMorale' }, personne.personneMorale)
    delete personne.personneMorale
  } else if (personne.personnePhysique) {
    Object.assign(personne, { type: 'personnePhysique' }, personne.personnePhysique)
    delete personne.personnePhysique
  }

  toArray(personne, 'prenom')
  fixKeys(personne, ['nationnalite'], 'nationalite')
  fixKeys(personne, ['numeroImmatriculation'], 'immatriculation')
  fixKeys(personne.immatriculation, ['numeroIdentificationRCS'], 'numeroIdentification')

  if (personne.capital) {
    if (personne.capital.montantCapital) {
      personne.capital.montant = Number(personne.capital.montantCapital)
      delete personne.capital.montantCapital
    }
    personne.capital.capitalVariable = personne.capital.capitalVariable !== undefined
  }

  if (personne.adresse) fixAdresse(personne.adresse)
  if (personne.etablissementPrincipal) fixAdresse(personne.etablissementPrincipal)
  if (personne.siegeSocial) fixAdresse(personne.siegeSocial)

  if (personne.immatriculation) {
    if (personne.immatriculation.numeroIdentification) personne.siren = personne.immatriculation.numeroIdentification.replace(/\s/g, '')
  }
  return personne
}

function extractPrixFond (origineFonds) {
  const origine = origineFonds.toLowerCase().replace(/\s/g, '')
  // number in form 30.000,00 or 30000,00 or 30000,0 (i.e descimal separated by ,)
  const match1 = origine.match(/((\d|\.)+,\d?\d)eur/)
  if (match1) return Number(match1[1].replace(/\./g, '').replace(/,/g, '.'))
  // number in form 30,000.00 or 30000.0 (i.e descimal separated by .)
  const match2 = origine.match(/((\d|,)+\.\d?\d)eur/)
  if (match2) return Number(match2[1].replace(/,/g, ''))
  // No decimal, get rid of all , and . chars
  const match3 = origine.replace(/(\.|,)/g, '').match(/(\d+)eur/)
  if (match3) return Number(match3[1])
}

function fixEtablissement (etab) {
  fixKeys(etab, ['qualiteEtablissement'], 'qualite')
  if (etab.adresse) etab.adresse.pays = 'france'
  if (etab.qualite) {
    const qualite = etab.qualite.toLowerCase()
    etab.siege = qualite.indexOf('principal') !== -1 || qualite.indexOf('siege') !== -1 || qualite.indexOf('siège') !== -1
  }
  if (etab.origineFonds) {
    const prix = extractPrixFond(etab.origineFonds)
    if (prix) {
      etab.prixFonds = prix
    }
  }
  return etab
}

// Fix date from format "13 janvier 2008" to "13-01-2008"
function fixDate (dateStr) {
  if (!dateStr) return undefined
  dateStr = dateStr.replace(/\s/g, ' ')
  dateStr = dateStr.replace(/1er/g, '1')
  const date = moment(dateStr, 'D MMM YYYY')
  if (date.isValid()) return date.format('YYYY-MM-DD')
  else return dateStr
}

function parseFile (name, type, tmpDir, cb) {
  const content = fs.readFileSync(path.join(tmpDir, name), 'utf-8')
  if (!content) {
    console.warn('File ' + name + ' is empty, this is weird !')
    return cb(null, [])
  }
  xml2js.parseString(content, xml2jsOpts, (err, xmlData) => {
    if (err) return cb(err)

    const root = xmlData[rootElements[type]]
    const liste = type !== 'PCL' ? root.listeAvis.avis : root.annonces.annonce
    delete (type !== 'PCL' ? root.listeAvis : root.annonces)
    if (!Array.isArray(liste)) {
      console.error('liste is not an array')
      return cb(null, [])
    }
    cb(null, liste
      .map(avis => {
        const avisStr = JSON.stringify(avis, null, 2)
        const data = Object.assign({}, root, { type, _id: [type, root.parution, avis.numeroAnnonce].join('_') })
        // remove xml namespace and stuff
        delete data.$
        try {
          data.numeroAnnonce = Number(avis.numeroAnnonce)
          delete avis.numeroAnnonce
          data.nojo = avis.nojo
          delete avis.nojo
          data.numeroDepartement = avis.numeroDepartement
          delete avis.numeroDepartement
          data.tribunal = avis.tribunal
          delete avis.tribunal

          if (avis.etablissement) {
            toArray(avis, 'etablissement')
            data.etablissements = avis.etablissement.filter(etab => typeof etab === 'object').map(fixEtablissement)
            delete avis.etablissement
          }
          if (avis.personnes) {
            toArray(avis.personnes, 'personne')
            data.personnes = avis.personnes.personne.map(fixPersonne)
            delete avis.personnes
          }
          // Cas des fichiers BILAN* ou la personne est à la racine de l'avis
          if (avis.numeroImmatriculation) {
            data.personnes = [fixPersonne({
              personneMorale: {
                adresse: avis.adresse,
                numeroImmatriculation: avis.numeroImmatriculation,
                denomination: avis.denomination,
                formeJuridique: avis.formeJuridique,
                sigle: avis.sigle
              }
            })]
            delete avis.adresse
            delete avis.numeroImmatriculation
            delete avis.denomination
            delete avis.formeJuridique
            delete avis.sigle

            // cas PCL
            if (avis.personneMorale) {
              data.personnes = [fixPersonne({
                personneMorale: {
                  denomination: avis.denomination,
                  formeJuridique: avis.formeJuridique
                }
              })]
              delete avis.numeroImmatriculation
              delete avis.denomination
              delete avis.formeJuridique
            }
          }
          if (avis.personnePhysique) {
            data.personnes = [fixPersonne({
              personnePhysique: {
                nom: avis.personnePhysique.nom,
                prenom: avis.personnePhysique.prenom
              }
            })]
            delete avis.nom
            delete avis.prenom
          }

          Object.keys(avis.acte || {}).forEach(type => {
            data.acte = Object.assign({ type }, avis.acte[type])
          })
          delete avis.acte
          fixKeys(data.acte, ['categorieVente', 'categorieImmatriculation', 'categorieCreation'], 'categorie')
          if (data.acte && !data.acte.categorie) {
            console.error('Weird acte : ' + JSON.stringify(data.acte.categorie, null, 2))
            data.acte.categorie = ''
          }

          if (!data.acte && avis.radiationAuRCS !== undefined) {
            data.acte = { type: 'radiation' }
            if (typeof avis.radiationAuRCS === 'string') {
              avis.radiationAuRCS = {}
            }
            if (avis.radiationAuRCS.radiationPP) {
              data.acte.dateCessationActivite = avis.radiationAuRCS.radiationPP.dateCessationActivitePP
              delete avis.radiationAuRCS.radiationPP.dateCessationActivitePP
              assertEmpty(avis.radiationAuRCS.radiationPP)
              delete avis.radiationAuRCS.radiationPP
            }
            if (avis.radiationAuRCS.radiationPM) {
              if (avis.radiationAuRCS.radiationPM !== 'O') console.error('Quelle valeur pour radiationPM ?', avis.radiationAuRCS.radiationPM)
              delete avis.radiationAuRCS.radiationPM
            }
            if (avis.radiationAuRCS.commentaire) {
              data.acte.commentaire = avis.radiationAuRCS.commentaire
              delete avis.radiationAuRCS.commentaire
            }
            assertEmpty(avis.radiationAuRCS)
            delete avis.radiationAuRCS
          }

          fixKeys(avis, ['modificationGenerale', 'modificationsGenerales'], 'modificationGenerale')
          if (!data.acte && avis.modificationGenerale !== undefined) {
            data.acte = { type: 'modificationGenerale', ...avis.modificationGenerale }
            moveKeys(data.acte, avis, ['precedentProprietairePM', 'precedentProprietairePP', 'precedentExploitantPM', 'precedentExploitantPP'])
            delete avis.modificationGenerale
          }

          if (!data.acte && avis.depot !== undefined) {
            data.acte = { type: 'depot', ...avis.depot }
            fixKeys(data.acte, ['typeDepot'], 'categorie')
            delete avis.depot
          }

          if (!data.acte && avis.typeAnnonce && avis.typeAnnonce.annulation !== undefined) data.acte = { type: 'annulation' }
          delete avis.typeAnnonce

          if (data.acte) {
            toArray(data.acte, 'opposition')
            data.acte.dateEffet = fixDate(data.acte.dateEffet)
            data.acte.dateCommencementActivite = fixDate(data.acte.dateCommencementActivite)
            data.acte.dateCessationActivite = fixDate(data.acte.dateCessationActivite)
          }

          if (avis.parutionAvisPrecedent) {
            data.parutionAvisPrecedent = avis.parutionAvisPrecedent
            fixKeys(data.parutionAvisPrecedent, ['numeroParution'], 'parution')
            data.parutionAvisPrecedent.dateParution = fixDate(data.parutionAvisPrecedent.dateParution)
            data.parutionAvisPrecedent.numeroAnnonce = Number(data.parutionAvisPrecedent.numeroAnnonce)
            data.parutionAvisPrecedent.type = { 'bodacc a': 'RCS-A', 'bodacc b': 'RCS-B', 'bodacc c': 'BILAN' }[data.parutionAvisPrecedent.nomPublication.replace(/\s/g, ' ').toLowerCase()] || data.parutionAvisPrecedent.nomPublication
            delete data.parutionAvisPrecedent.nomPublication
            data.parutionAvisPrecedent._id = [data.parutionAvisPrecedent.type, data.parutionAvisPrecedent.parution, data.parutionAvisPrecedent.numeroAnnonce].join('_')
            delete avis.parutionAvisPrecedent
          }

          if (avis.precedentProprietairePM) {
            toArray(avis, 'precedentProprietairePM')
            data.precedentProprietaires = avis.precedentProprietairePM.map(p => fixPersonne({ personneMorale: p }))
            delete avis.precedentProprietairePM
          }

          if (avis.precedentProprietairePP) {
            toArray(avis, 'precedentProprietairePP')
            data.precedentProprietaires = avis.precedentProprietairePP.map(p => fixPersonne({ personneMorale: p }))
            delete avis.precedentProprietairePP
          }

          if (avis.precedentExploitantPM) {
            toArray(avis, 'precedentExploitantPM')
            data.precedentExploitants = avis.precedentExploitantPM.map(p => fixPersonne({ personneMorale: p }))
            delete avis.precedentExploitantPM
          }

          if (avis.precedentExploitantPP) {
            toArray(avis, 'precedentExploitantPP')
            data.precedentExploitants = avis.precedentExploitantPP.map(p => fixPersonne({ personneMorale: p }))
            delete avis.precedentExploitantPP
          }
          if (avis.identifiantClient) {
            toArray(avis, 'identifiantClient')
            data.precedentExploitants = avis.identifiantClient
            delete avis.identifiantClient
          }
          if (avis.activite) {
            toArray(avis, 'activite')
            data.activite = avis.activite
            delete avis.activite
          }
          if (avis.jugement) {
            toArray(avis, 'jugement')
            data.jugement = avis.jugement
            delete avis.jugement
          }
          if (avis.personneMorale) {
            data.personneMorale = avis.personneMorale
            delete avis.personneMorale
          }
          if (avis.personnePhysique) {
            data.personnesPhysique = avis.personnesPhysique
            delete avis.personnePhysique
          }
          if (avis.nonInscrit) {
            data.nonInscrit = avis.nonInscrit
            delete avis.nonInscrit
          }
          if (avis.adresse) {
            data.adresse = avis.adresse
            delete avis.adresse
          }
          if (avis.enseigne) {
            data.enseigne = avis.enseigne
            delete avis.enseigne
          }
          if (avis.inscriptionRM) {
            data.inscriptionRM = avis.inscriptionRM
            delete avis.inscriptionRM
          }
          if (avis.jugementAnnule) {
            data.jugementAnnule = avis.jugementAnnule
            delete avis.jugementAnnule
          }
          if (avis.numeroImmatriculation){
            data.numeroImmatriculation.numeroIdentificationRCS = avis.numeroIdentification
            data.numeroImmatriculation.codeRCS = avis.codeRCS
            data.numeroImmatriculation.nomGreffeImmat = avis.nomGreffeImmat
            delete avis.numeroImmatriculation
          }

          assertEmpty(avis)
        } catch (err) {
          console.log(err) // + avisSTR
        }

        Object.keys(data).forEach(function (key) {
          if (tab.includes(key) === false) {
            tab.push(key)
          }
        })
        return { annonce: data, raw: avisStr }
      })
    )
  })
}
const tab = []
function parseRCSA (item, type) {
  const ret = {
    // etablissements
    origineFonds: '',
    activite: '',
    adresseEtab_pays: '',
    adresseEtab_numeroVoie: '',
    adresseEtab_typeVoie: '',
    adresseEtab_nomVoie: '',
    adresseEtab_complGeographique: '',
    adresseEtab_codePostal: '',
    adresseEtab_ville: '',
    qualite: '',
    siege: false,
    prixFonds: '',
    // personnes
    capital_devise: '',
    capital_montant: '',
    capital_capitalVariable: '',
    adressePers_pays: '',
    adressePers_numeroVoie: '',
    adressePers_typeVoie: '',
    adressePers_nomVoie: '',
    adressePers_complGeographique: '',
    adressePers_codePostal: '',
    adressePers_ville: '',
    typePers: '',
    immatriculation_numeroIdentification: '',
    immatriculation_codeRCS: '',
    immatriculation_nomGreffeImmat: '',
    nonInscrit: '',
    denomination: '',
    formeJuridique: '',
    administration: '',
    // actes
    typeActe: item.acte.type,
    dateImmatriculation: item.acte.dateImmatriculation,
    dateCommencementActivite: item.acte.dateCommencementActivite,
    categorie: item.acte.categorie,
    dateEffet: item.acte.dateEffet,
    dateCessationActivite: item.acte.dateCessationActivite
  }

  if (item.etablissements && item.etablissements.length) {
    ret.origineFonds = item.etablissements[0].origineFonds
    ret.activite = item.etablissements[0].activite
    if (item.etablissements.adresse) {
      ret.adresseEtab_pays = item.etablissement[0].adresse.pays
      ret.adresseEtab_numeroVoie = item.etablissement[0].adresse.numeroVoie
      ret.adresseEtab_typeVoie = item.etablissement[0].adresse.typeVoie
      ret.adresseEtab_nomVoie = item.etablissement[0].adresse.nomVoie
      ret.adresseEtab_complGeographique = item.etablissement[0].adresse.complGeographique
      ret.adresseEtab_codePostal = item.etablissement[0].adresse.codePostal
      ret.adresseEtab_ville = item.etablissement[0].adresse.ville
    }
    ret.qualite = item.etablissements[0].qualite
    ret.siege = item.etablissements[0].siege
    ret.prixFonds = item.etablissements[0].prixFonds
  }

  if (item.personnes && item.personnes.length) {
    ret.capital_devise = item.personnes[0].capital ? item.personnes[0].capital.devise : ''
    ret.capital_montant = item.personnes[0].capital ? item.personnes[0].capital.montant : ''
    ret.capital_capitalVariable = item.personnes[0].capital ? item.personnes[0].capital.capitalVariable : ''
    if (item.personnes[0].adresse) {
      if (item.personnes[0].adresse.etranger) {
        ret.adresseSiegeSocial_complGeographique = item.personnes[0].adresse.etranger.adresse
        ret.adresseSiegeSocial_pays = item.personnes[0].adresse.etranger.pays ? item.personnes[0].adresse.etranger.pays : ''
      }
      ret.adressePers_pays = item.personnes[0].adresse.pays
      ret.adressePers_numeroVoie = item.personnes[0].adresse.numeroVoie
      ret.adressePers_typeVoie = item.personnes[0].adresse.typeVoie
      ret.adressePers_nomVoie = item.personnes[0].adresse.nomVoie
      ret.adressePers_complGeographique = item.personnes[0].adresse.complGeographique
      ret.adressePers_codePostal = item.personnes[0].adresse.codePostal
      ret.adressePers_ville = item.personnes[0].adresse.ville
    }
    ret.typePers = item.personnes[0].type
    ret.nonInscrit = item.personnes[0].nonInscrit ? item.personnes[0].nonInscrit : ''
    ret.denomination = item.personnes[0].denomination
    ret.formeJuridique = item.personnes[0].formeJuridique ? item.personnes[0].formeJuridique : ''
    ret.administration = item.personnes[0].administration ? item.personnes[0].administration : ''
    if (item.personnes[0].immatriculation) {
      ret.immatriculation_numeroIdentification = item.personnes[0].immatriculation.numeroIdentification
      ret.immatriculation_codeRCS = item.personnes[0].immatriculation.codeRCS
      ret.immatriculation_nomGreffeImmat = item.personnes[0].immatriculation.nomGreffeImmat
    }
  }

  if (ret.typeActe.toUpperCase().includes(type.toUpperCase())) return ret
}
function parseRCSB (item) {
  const ret = {
    // Personnes
    activite: '',
    adresseSiegeSocial_pays: '',
    adresseSiegeSocial_numeroVoie: '',
    adresseSiegeSocial_typeVoie: '',
    adresseSiegeSocial_nomVoie: '',
    adresseSiegeSocial_complGeographique: '',
    adresseSiegeSocial_codePostal: '',
    adresseSiegeSocial_ville: '',
    adresseEtab_pays: '',
    adresseEtab_numeroVoie: '',
    adresseEtab_typeVoie: '',
    adresseEtab_nomVoie: '',
    adresseEtab_complGeographique: '',
    adresseEtab_codePostal: '',
    adresseEtab_ville: '',
    typePers: '',
    nomPers: '',
    prenomPers: '',
    adressePers_pays: '',
    adressePers_numeroVoie: '',
    adressePers_typeVoie: '',
    adressePers_nomVoie: '',
    adressePers_complGeographique: '',
    adressePers_codePostal: '',
    adressePers_ville: '',
    denomination: '',
    formeJuridique: '',
    administration: '',
    capital_devise: '',
    capital_montant: '',
    capital_capitalVariable: '',
    sigle: '',
    immatriculation_numeroIdentification: '',
    immatriculation_codeRCS: '',
    immatriculation_nomGreffeImmat: '',
    siren: '',

    // Acte
    typeActe: item.acte.type,
    descriptif: item.acte.descriptif,
    dateEffet: item.acte.dateEffet,
    dateCommencementActivite: item.acte.dateCommencementActivite,
    dateCessationActivite: item.acte.dateCessationActivite
  }
  if (item.personnes && item.personnes.length) {
    ret.activite = item.personnes[0].activite
    ret.typePers = item.personnes[0].type
    ret.nomPers = item.personnes[0].nom
    ret.prenomPers = item.personnes[0].prenom ? item.personnes[0].prenom[0] : undefined
    ret.denomination = item.personnes[0].denomination
    ret.formeJuridique = item.personnes[0].formeJuridique
    ret.administration = item.personnes[0].administration
    ret.capital_devise = item.personnes[0].capital ? item.personnes[0].capital.devise : ''
    ret.capital_montant = item.personnes[0].capital ? item.personnes[0].capital.montant : ''
    ret.capital_capitalVariable = item.personnes[0].capital ? item.personnes[0].capital.capitalVariable : ''
    ret.sigle = item.personnes[0].sigle
    if (item.personnes[0].immatriculation) {
      ret.immatriculation_numeroIdentification = item.personnes[0].immatriculation.numeroIdentification
      ret.immatriculation_codeRCS = item.personnes[0].immatriculation.codeRCS
      ret.immatriculation_nomGreffeImmat = item.personnes[0].immatriculation.nomGreffeImmat
    }
    ret.siren = item.personnes[0].siren
    // SiegeSocial
    if (item.personnes[0].siegeSocial) {
      if (item.personnes[0].siegeSocial.etranger) {
        ret.adresseSiegeSocial_complGeographique = item.personnes[0].siegeSocial.etranger.adresse
        ret.adresseSiegeSocial_pays = item.personnes[0].siegeSocial.etranger.pays ? item.personnes[0].siegeSocial.etranger.pays : ''
      }
      if (item.personnes[0].siegeSocial.france) {
        ret.adresseSiegeSocial_pays = item.personnes[0].siegeSocial.pays
        ret.adresseSiegeSocial_numeroVoie = item.personnes[0].siegeSocial.france.numeroVoie
        ret.adresseSiegeSocial_typeVoie = item.personnes[0].siegeSocial.france.typeVoie
        ret.adresseSiegeSocial_nomVoie = item.personnes[0].siegeSocial.france.nomVoie
        ret.adresseSiegeSocial_complGeographique = item.personnes[0].siegeSocial.france.complGeographique ? item.personnes[0].siegeSocial.france.complGeographique : item.personnes[0].siegeSocial.france.localite
        ret.adresseSiegeSocial_codePostal = item.personnes[0].siegeSocial.france.codePostal
        ret.adresseSiegeSocial_ville = item.personnes[0].siegeSocial.france.ville
      }
    }
    // Etablissement
    if (item.personnes[0].etablissementPrincipal) {
      ret.adresseEtab_pays = item.personnes[0].etablissementPrincipal.pays
      if (item.personnes[0].etablissementPrincipal.france) {
        ret.adresseEtab_numeroVoie = item.personnes[0].etablissementPrincipal.france.numeroVoie
        ret.adresseEtab_typeVoie = item.personnes[0].etablissementPrincipal.france.typeVoie
        ret.adresseEtab_nomVoie = item.personnes[0].etablissementPrincipal.france.nomVoie
        ret.adresseEtab_complGeographique = item.personnes[0].etablissementPrincipal.france.complGeographique
        ret.adresseEtab_codePostal = item.personnes[0].etablissementPrincipal.france.codePostal
        ret.adresseEtab_ville = item.personnes[0].etablissementPrincipal.france.ville
      }
    }
    if (item.personnes[0].adresse) {
      ret.adressePers_pays = item.personnes[0].adresse.pays
      ret.adressePers_numeroVoie = item.personnes[0].adresse.numeroVoie
      ret.adressePers_nomVoie = item.personnes[0].adresse.nomVoie
      ret.adressePers_typeVoie = item.personnes[0].adresse.typeVoie
      ret.adressePers_complGeographique = item.personnes[0].adresse.complGeographique
      ret.adressePers_codePostal = item.personnes[0].adresse.codePostal
      ret.adressePers_ville = item.personnes[0].adresse.ville
    }
  }
  return ret
}

function parseBILAN (item) {
  const ret = {
    typePers: '',
    adressePers_pays: '',
    adressePers_numeroVoie: '',
    adressePers_typeVoie: '',
    adressePers_nomVoie: '',
    adressePers_complGeographique: '',
    adressePers_codePostal: '',
    adressePers_ville: '',
    denomination: '',
    formeJuridique: '',
    sigle: '',
    immatriculation_numeroIdentification: '',
    immatriculation_codeRCS: '',
    immatriculation_nomGreffeImmat: '',
    siren: '',
    typeActe: item.acte.type,
    dateCloture: item.acte.dateCloture,
    descriptif: item.acte.descriptif,
    categorie: item.acte.categorie,
    dateEffet: item.acte.dateEffet,
    dateCommencementActivite: item.acte.dateCommencementActivite,
    dateCessationActivite: item.acte.dateCessationActivite
  }
  if (item.personnes && item.personnes.length) {
    ret.typePers = item.personnes[0].type
    if (item.personnes[0].adresse) {
      if (item.personnes[0].adresse.etranger) {
        ret.adressePers_complGeographique = item.personnes[0].adresse.etranger.adresse
        ret.adressePers_pays = item.personnes[0].adresse.etranger.pays ? item.personnes[0].adresse.etranger.pays : ''
      }
      if (item.personnes[0].adresse.france) {
        ret.adressePers_pays = item.personnes[0].adresse.pays
        ret.adressePers_numeroVoie = item.personnes[0].adresse.france.numeroVoie
        ret.adressePers_nomVoie = item.personnes[0].adresse.france.nomVoie
        ret.adressePers_typeVoie = item.personnes[0].adresse.france.typeVoie
        ret.adressePers_complGeographique = item.personnes[0].adresse.france.complGeographique ? item.adresse.france.complGeographique : item.adresse.france.localite
        ret.adressePers_codePostal = item.personnes[0].adresse.france.codePostal
        ret.adressePers_ville = item.personnes[0].adresse.france.ville
      }
    }
    ret.denomination = item.personnes[0].denomination
    ret.formeJuridique = item.personnes[0].formeJuridique
    ret.sigle = item.personnes[0].sigle
    if (item.personnes[0].immatriculation) {
      ret.immatriculation_numeroIdentification = item.personnes[0].immatriculation.numeroIdentification
      ret.immatriculation_codeRCS = item.personnes[0].immatriculation.codeRCS
      ret.immatriculation_nomGreffeImmat = item.personnes[0].immatriculation.nomGreffeImmat
    }
    ret.siren = item.personnes[0].siren
  }
  return ret
}

function parsePCL (item) {
  // if (item.annonce.nojo === '002021123000064') console.log(item)
  const ret = {
    typePers: '',
    nomPers: '',
    prenomPers: '',
    adressePers_pays: '',
    adressePers_numeroVoie: '',
    adressePers_typeVoie: '',
    adressePers_nomVoie: '',
    adressePers_complGeographique: '',
    adressePers_codePostal: '',
    adressePers_ville: '',
    immatriculation_numeroIdentification: '',
    immatriculation_codeRCS: '',
    immatriculation_nomGreffeImmat: '',
    formeJuridique: '',
    denomination: '',
    nonInscrit: '',
    identifiantClient: '', // Precedents exploitants
    siren: '',
    activite: '',
    jugement_famille: '',
    jugement_nature: '',
    jugement_date: '',
    jugement_complementJugement: '',
    enseigne: '',
    inscription_numeroIdentification: '',
    inscription_code: '',
    inscription_numeroDepartement: '',
    inscription_nomGreffeImmat: '',
    parutionAvisPrecedent_dateParution: '',
    parutionAvisPrecedent_numeroAnnonce: '',
    parutionAvisPrecedent_parution: '',
    parutionAvisPrecedent_type: '',
    parutionAvisPrecedent__id: '',
    jugementAnnule_famille: '',
    jugementAnnule_nature: ''
  }
  
  ret.nonInscrit = item.nonInscrit ? item.nonInscrit : undefined
  if (item.adresse) {
    if (item.adresse.etranger) {
      ret.adressePers_complGeographique = item.adresse.etranger.adresse
      ret.adressePers_pays = item.adresse.etranger.pays ? item.adresse.etranger.pays : item.adresse.pays
    }
    if (item.adresse.france) {
      ret.adressePers_pays = item.adresse.pays
      ret.adressePers_numeroVoie = item.adresse.france.numeroVoie
      ret.adressePers_typeVoie = item.adresse.france.typeVoie
      ret.adressePers_nomVoie = item.adresse.france.nomVoie
      ret.adressePers_complGeographique = item.adresse.france.complGeographique ? item.adresse.france.complGeographique : item.adresse.france.localite
      ret.adressePers_codePostal = item.adresse.france.codePostal
      ret.adressePers_ville = item.adresse.france.ville
    }
  }
  
  if (item.jugement) {
    ret.jugement_famille = item.jugement[0].famille
    ret.jugement_nature = item.jugement[0].nature
    ret.jugement_date = item.jugement[0].date
    ret.jugement_complementJugement = item.jugement[0].complementJugement
  }
  if (item.personnes && item.personnes.lentgh) {
    ret.typePers = item.personnes[0]
    if (ret.typePers.match('Morale')) {
      const pers = ret.typePers
      ret.denomination = item.pers.denomination
      ret.formeJuridique = item.pers.formeJuridique
    }
    if (ret.typePers.match('Physique')) {
      ret.nomPers = item.personnes[0].nom
      ret.prenomPers = item.personnes[0].prenom[0]
    }
    if (item.personnes[0].immatriculation) {
      ret.immatriculation_numeroIdentification = item.personnes[0].immatriculation.numeroIdentification
      ret.immatriculation_codeRCS = item.personnes[0].immatriculation.codeRCS
      ret.immatriculation_nomGreffeImmat = item.personnes[0].immatriculation.nomGreffeImmat
    }
  }
  ret.enseigne = item.enseigne ? item.enseigne : undefined
  if (item.inscriptionRM) {
    ret.inscription_numeroIdentification = item.inscriptionRM.numeroIdentificationRM
    ret.inscription_code = item.inscriptionRM.codeRM
    ret.inscription_numeroDepartement = item.inscriptionRM.numeroDepartement
  }
  if (item.inscriptionRCS) {
    ret.inscription_numeroIdentification = item.personnes[0].immatriculation.numeroIdentification
    ret.inscription_code = item.personnes[0].immatriculation.codeRCS
    ret.inscription_nomGreffeImmat = item.personnes[0].immatriculation.nomGreffeImmat
  }
  if ( item.parutionAvisPrecedent) {
    ret.parutionAvisPrecedent_dateParution = item.parutionAvisPrecedent.dateParution
    ret.parutionAvisPrecedent_numeroAnnonce = item.parutionAvisPrecedent.numeroAnnonce
    ret.parutionAvisPrecedent_parution = item.parutionAvisPrecedent.parution
    ret.parutionAvisPrecedent_type = item.parutionAvisPrecedent.type
    ret.parutionAvisPrecedent__id = item.parutionAvisPrecedent._id
  }
  if (item.jugementAnnule) {
    ret.jugementAnnule_famille = item.jugementAnnule.famille
    ret.jugementAnnule_nature = item.jugementAnnule.nature
  }
  
  return ret
}

function processFile (type, stream, tmpDir, processingConfig) {
  return function (name, cb) {
    // console.log(name)
    parseFile(name, type, tmpDir, (err, items) => {
      if (err) return cb(err)
      async.map(items, (item, cb) => {
        const base = {
          parution: item.annonce.parution,
          dateParution: item.annonce.dateParution,
          type: item.annonce.type,
          _id: item.annonce._id,
          numeroAnnonce: item.annonce.numeroAnnonce,
          nojo: item.annonce.nojo,
          numeroDepartement: item.annonce.numeroDepartement,
          tribunal: item.annonce.tribunal
        }

        let itemType
        try {
          if (type === 'RCS-A') itemType = parseRCSA(item.annonce, processingConfig.typeFile)
          if (type === 'RCS-B') itemType = parseRCSB(item.annonce)
          if (type === 'BILAN') itemType = parseBILAN(item.annonce)
          if (type === 'PCL') itemType = parsePCL(item.annonce)
        } catch (err) {
          console.log(err)
          throw err
        }

        if (itemType) {
          const ret = {
            ...base,
            ...itemType
          }
          stream.write(csv.stringify([ret], { quoted_string: true }))
        }
      }, (err) => {
        if (err) {
          console.log(err)
          return cb(err)
        }
      })
    })
  }
}

// Ventes, cessions, créations d'établissements, etc..
async function processFiles (tmpDir, type, processingConfig, log) {
  await log.info(`Traitement du fichier BODAAC : ${type}. Type : ${processingConfig.typeFile}`)
  const globalHeader = require('./schema_global.json').map((elem) => elem.key)
  const typeHeader = require(`./schema_${type}.json`).map((elem) => elem.key)

  const header = globalHeader.concat(typeHeader)
  const writeStream = fs.createWriteStream(path.join(tmpDir, processingConfig.typeFile.toUpperCase() + '.csv'))
  writeStream.write(header.map((elem) => `"${elem}"`).join(',') + endOfLine)
  const rcsFiles = fs.readdirSync(tmpDir).filter(file => file.indexOf(type) === 0 && file.indexOf('.xml') !== -1)

  console.log(`found ${rcsFiles.length} ${type} files`)
  rcsFiles.map(function (file) {
    processFile(type, writeStream, tmpDir, processingConfig)(file, null)
    return null
  })
  async function waitForStreamClose (stream) {
    stream.close()
    return new Promise((resolve, reject) => {
      stream.once('close', () => {
        resolve()
      })
    })
  }
  await waitForStreamClose(writeStream)
}

module.exports = async (processingConfig, tmpDir, axios, log, patchConfig) => {
  await log.info('Début du traitement')
  if (processingConfig.typeFile === 'modification') await processFiles(tmpDir, 'RCS-B', processingConfig, log)
  else if (processingConfig.typeFile === 'compte') await processFiles(tmpDir, 'BILAN', processingConfig, log)
  else if (processingConfig.typeFile === 'prevention') await processFiles(tmpDir, 'PCL', processingConfig, log)
  else await processFiles(tmpDir, 'RCS-A', processingConfig, log)
}
