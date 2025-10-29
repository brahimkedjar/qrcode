'use client';
import { useState, useEffect } from 'react';
import axios from 'axios';
import PaymentForm from '../demande/step9/PaymentForm';
import PaymentsTable from '../demande/step9/PaymentsTable';
import styles from './Payments.module.css';
import { useSearchParams } from '@/src/hooks/useSearchParams';
import { FiChevronLeft, FiChevronRight, FiDownload, FiCheck } from 'react-icons/fi';
import Sidebar from '../sidebar/Sidebar';
import Navbar from '../navbar/Navbar';
import { useViewNavigator } from '../../src/hooks/useViewNavigator';
import { STEP_LABELS } from '../../src/constants/steps';
import router from 'next/router';
import { useActivateEtape } from '@/src/hooks/useActivateEtape';
import TaxeSuperficiaireSection from '../demande/step9/TaxeSuperficiaireSection';
import { generatePDFForPreview, generateUniqueOrderNumber } from '../../utils/pdfGenerator';
import PDFPreviewModal from './PDFPreviewModal';

interface Obligation {
  id: number;
  details_calcul: string | null;
  typePaiement: {
    id: number;
    libelle: string;
  };
  amount: number;
  fiscalYear: number;
  dueDate: string;
  status: string;
  payments: RawPayment[];
  tsPaiements?: TsPaiement[]; // Add TsPaiement data
}

interface TsPaiement {
  id_tsPaiement: number;
  datePerDebut: string;
  datePerFin: string;
  surfaceMin: number;
  surfaceMax: number;
}

interface RawPayment {
  id: number;
  paymentDate: string;
  amount: number;
  paymentMethod: string;
  receiptNumber: string;
  status: string;
  proofUrl: string | null;
  currency: string;
}

const PaymentPage = () => {
  const searchParams = useSearchParams();
  const permisIdStr = searchParams?.get('permisId');
  const permisId = permisIdStr ? parseInt(permisIdStr, 10) : undefined;
  const [obligations, setObligations] = useState<Obligation[]>([]);
  const [selectedObligation, setSelectedObligation] = useState<Obligation | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [totalAmount, setTotalAmount] = useState<number>(0);
  const [refreshKey, setRefreshKey] = useState(0);
  const { currentView, navigateTo } = useViewNavigator('nouvelle-demande')
  const currentStep = 9;
  const totalSteps = STEP_LABELS.length;
  const progress = ((currentStep + 1) / totalSteps) * 100;
  const [statutProc, setStatutProc] = useState<string | undefined>(undefined);
  const apiURL = process.env.NEXT_PUBLIC_API_URL; 
  const [permisDetails, setPermisDetails] = useState<any>(null);
  const [dateAttribution, setDateAttribution] = useState<Date>(new Date());    
  const statusStyles: Record<string, string> = {
    'Payé': styles.paidStatus,
    'En retard': styles.overdueStatus,
    'Partiellement payé': styles.partialStatus,
    'A payer': styles.pendingStatus,
  };
  const [generatingOrder, setGeneratingOrder] = useState<string | null>(null);
  const [selectedForDownload, setSelectedForDownload] = useState<{
    DEA: number | null;
    TS: number | null;
    PRODUIT_ATTRIBUTION: number | null;
  }>({
    DEA: null,
    TS: null,
    PRODUIT_ATTRIBUTION: null
  });

  
  useEffect(() => {
    if (obligations.length > 0) {
      const deaObligation = obligations.find(ob => 
        ob.typePaiement.libelle.includes('Droit d\'établissement') || 
        ob.typePaiement.libelle.includes('DEA')
      );
      
      const tsObligation = obligations.find(ob => 
        ob.typePaiement.libelle.includes('Taxe superficiaire') || 
        ob.typePaiement.libelle.includes('TS')
      );
      
      const produitObligation = obligations.find(ob => 
        ob.typePaiement.libelle.includes('Produit d\'attribution') || 
        ob.typePaiement.libelle.includes('Attribution')
      );
      
      setSelectedForDownload({
        DEA: deaObligation?.id || null,
        TS: tsObligation?.id || null,
        PRODUIT_ATTRIBUTION: produitObligation?.id || null
      });
    }
  }, [obligations]);

  // Update the initializePayments function to fetch TsPaiement data
  useEffect(() => {
    const initializePayments = async () => {
      if (!permisId) {
        setLoading(false);
        setError('Aucun permis sélectionné. Veuillez ouvrir un permis depuis le tableau des paiements.');
        setPermisDetails(null);
        setObligations([]);
        return;
      }

      try {
        setLoading(true);
        setError(null);
        
        // Get permis details directly by permisId
        const permisResponse = await axios.get(`${apiURL}/payments/permis/${permisId}`);
        
        if (!permisResponse.data) throw new Error('Permis non trouvé');

        setPermisDetails(permisResponse.data);
        
        // Get attribution date from permis if available, otherwise use current date
        const attributionDate = permisResponse.data.date_octroi 
          ? new Date(permisResponse.data.date_octroi)
          : new Date();
        setDateAttribution(attributionDate);

        // Check if obligations exist
        const obligationsResponse = await axios.get(
          `${apiURL}/payments/obligations/${permisId}`
        );
        
        // Get TsPaiement data for surface tax obligations
        const obligationsWithTsPaiement = await Promise.all(
          obligationsResponse.data.map(async (obligation: any) => {
            if (obligation.typePaiement?.libelle === 'Taxe superficiaire') {
              try {
                const tsPaiementResponse = await axios.get(
                  `${apiURL}/payments/ts-paiement/obligation/${obligation.id}`
                );
                return {
                  ...obligation,
                  tsPaiements: tsPaiementResponse.data
                };
              } catch (error) {
                console.error('Error fetching TsPaiement data:', error);
                return obligation;
              }
            }
            return obligation;
          })
        );
        
        setObligations(obligationsWithTsPaiement);
        
        // Also set the first obligation as selected if available
        if (obligationsWithTsPaiement.length > 0) {
          setSelectedObligation(obligationsWithTsPaiement[0]);
        }
        
      } catch (err) {
        console.error('Error in initializePayments:', err);
        setError(err instanceof Error ? err.message : 'échec de l\'initialisation');
      } finally {
        setLoading(false);
      }
    };

    initializePayments();
  }, [permisId]);

  // Add helper function to format dates
  const formatDateUTC = (dateString: string | Date) => {
    const date = new Date(dateString);
    const day = String(date.getUTCDate()).padStart(2, "0");
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const year = date.getUTCFullYear();
    return `${day}/${month}/${year}`;
  };

  // Add function to get period display for Taxe Superficiaire
  const getPeriodDisplay = (obligation: Obligation) => {
    if (obligation.typePaiement.libelle !== "Taxe superficiaire") {
      return <span className={styles.fiscalYear}>{obligation.fiscalYear}</span>;
    }

    if (obligation.tsPaiements && obligation.tsPaiements.length > 0) {
      const tsPaiement = obligation.tsPaiements[0];
      return (
        <div className={styles.periodDisplay}>
          <div className={styles.dateBox}>
            <span className={styles.label}>Début</span>
            <span className={styles.date}>{formatDateUTC(tsPaiement.datePerDebut)}</span>
          </div>
          <span className={styles.arrow}>→</span>
          <div className={styles.dateBox}>
            <span className={styles.label}>Fin</span>
            <span className={styles.date}>{formatDateUTC(tsPaiement.datePerFin)}</span>
          </div>
        </div>
      );
    }

    return <span className={styles.fiscalYear}>{obligation.fiscalYear}</span>;
  };


useEffect(() => {
  if (obligations.length > 0) {
    const surfaceTax = obligations.filter(ob => 
      ob.typePaiement?.libelle === 'Taxe superficiaire'
    );
    
    console.log('DEBUG - Surface tax obligations:', surfaceTax);
    surfaceTax.forEach(ob => {
      console.log(`Obligation ${ob.id}:`, {
        details_calcul: ob.details_calcul,
        parsed: ob.details_calcul ? JSON.parse(ob.details_calcul) : 'No details'
      });
    });
  }
}, [obligations]);
  /*useEffect(() => {
    if (!idProc) return;
    const activateStep = async () => {
      try {
        await axios.post(`${apiURL}/api/procedure-etape/start/${idProc}/10`);
      } catch (err) {
        console.error("échec de l'activation de l'étape");
      }
    };

    activateStep();
  }, [idProc]);*/

    const handleBack = () => {
    if (!permisId) {
      setError("ID procédure manquant");
      return;
    }
    router.push(`/DEA/DEA_dashboard`);
  };
    
  useEffect(() => {
    if (obligations.length > 0) {
      const total = obligations.reduce((sum, obligation) => {
        return sum + (obligation.amount || 0);
      }, 0);
      setTotalAmount(total);
    }
  }, [obligations]);

  const fetchPayments = async (obligationId: number) => {
    try {
      const response = await axios.get<RawPayment[]>(
        `${apiURL}/payments/payments/${obligationId}`
      );

      setSelectedObligation((prev) =>
        prev ? { ...prev, payments: response.data } : null
      );

      setObligations((prev) =>
        prev.map((obligation) =>
          obligation.id === obligationId
            ? { ...obligation, payments: response.data }
            : obligation
        )
      );
    } catch (error) {
      setError('Erreur lors du chargement de l\'historique');
    }
  };

  const handleObligationSelect = async (obligation: Obligation) => {
    setSelectedObligation(obligation);
    await fetchPayments(obligation.id);
  };



  const handlePaymentSubmit = async (paymentData: {
    amount: number;
    currency: string;
    paymentDate: string;
    paymentMethod: string;
    receiptNumber: string;
    proofUrl: string;
  }) => {
    try {
      if (!selectedObligation) return;

      await axios.post(`${apiURL}/payments`, {
        ...paymentData,
        obligationId: selectedObligation.id,
      });

      await fetchPayments(selectedObligation.id);

      const obligationsResponse = await axios.get<Obligation[]>(
        `${apiURL}/payments/obligations/${permisId}`
      );

      setObligations(obligationsResponse.data);
console.log('Updated obligations after payment:', obligationsResponse.data);
      const updated = obligationsResponse.data.find(o => o.id === selectedObligation.id);
      setSelectedObligation(updated || obligationsResponse.data[0]);
    } catch (error) {
      setError('échec de la soumission du paiement');
    }
  };

  if (error) {
    return (
      <div className={styles.container}>
        <h1 className={styles.title}>Paiements requis</h1>
        <div className={styles.error}>{error}</div>
      </div>
    );
  }

   const [previewModalOpen, setPreviewModalOpen] = useState(false);
const [currentPdfData, setCurrentPdfData] = useState<string>('');
const [currentPdfType, setCurrentPdfType] = useState<'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION'>('DEA');
const [currentOrderData, setCurrentOrderData] = useState<any>(null);

// In your PaymentPage component, update the handleGenerateOrder function
 const handleGenerateOrder = async (type: 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION') => {
    if (!permisDetails) {
      setError("Détails du permis manquants");
      return;
    }

    try {
      setGeneratingOrder(type);
      
      // Use the selected obligation for this type
      const obligationId = selectedForDownload[type];
      if (!obligationId) {
        setError(`Aucune obligation ${type} sélectionnée`);
        setGeneratingOrder(null);
        return;
      }
      
      const obligation = obligations.find(ob => ob.id === obligationId);
      if (!obligation) {
        setError(`Obligation ${type} non trouvée`);
        setGeneratingOrder(null);
        return;
      }
      
      // Extract company information from the API response
      const companyName = permisDetails.detenteur?.nom_societeFR || 
                         permisDetails.detenteur?.nom_societeAR || 
                         "LA GRANDE SABLIERE DE BOUSAADA";
      
      // Extract permit information
      const permitType = permisDetails.typePermis?.libelle || "Permis d'exploitation de carriéres";
      const permitCode = permisDetails.code_permis || `PXC-${permisId}`;
      
      // Extract location information
      const location = permisDetails.detenteur?.adresse_siege || 
                      permisDetails.commune?.nom_commune_fr || 
                      "Bousaada";
      
      // Extract detenteur ID for unique order number
      const detenteurId = permisDetails.detenteur?.id || 0;
      
      // Calculate the period based on TsPaiement data for TS orders
      let period = "13/07/2025 au 31/12/2025"; // Default
      
      if (type === 'TS' && obligation.tsPaiements && obligation.tsPaiements.length > 0) {
        const tsPaiement = obligation.tsPaiements[0];
        period = `${formatDateUTC(tsPaiement.datePerDebut)} au ${formatDateUTC(tsPaiement.datePerFin)}`;
      } else if (type === 'TS') {
        // If no TsPaiement data, calculate based on permit dates
        if (permisDetails.date_octroi) {
          const startDate = new Date(permisDetails.date_octroi);
          const endDate = new Date(startDate);
          endDate.setFullYear(endDate.getFullYear() + 1); // One year from attribution
          
          period = `${formatDateUTC(startDate)} au ${formatDateUTC(endDate)}`;
        }
      }
      
      // Generate unique order number
      const orderNumber = generateUniqueOrderNumber(
        type,
        detenteurId,
        obligation.id,
        permisId || 0
      );
      
      const orderData = {
        companyName,
        permitType,
        permitCode,
        location,
        amount: obligation.amount,
        orderNumber,
        date: new Date(),
        taxReceiver: "receveur des impôts de Didouche Mourad\n",
        taxReceiverAddress: "17 rue Arezki Hammani, 3éme étage-Alger",
        period: type === 'TS' ? period : undefined,
        detenteurId,
        obligationId: obligation.id,
        president: "P/Le Président du Comité de Direction",
        signatureName: "Seddik BENABBES" 
      };
      
      // Generate PDF for preview
      const pdfDataUrl = await generatePDFForPreview(type, orderData);
      
      // Set states for preview modal
      setCurrentPdfData(pdfDataUrl);
      setCurrentPdfType(type);
      setCurrentOrderData(orderData);
      setPreviewModalOpen(true);
      
      // Show success message
      setError(null);
      
    } catch (error) {
      console.error("Error generating PDF:", error);
      setError("Erreur lors de la génération du document");
    } finally {
      setGeneratingOrder(null);
    }
  };


const handleSelectForDownload = (obligationId: number, type: 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION') => {
    setSelectedForDownload(prev => ({
      ...prev,
      [type]: obligationId
    }));
  };

  // Add this helper function to get obligation type
  const getObligationType = (obligation: Obligation): 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION' | null => {
    if (obligation.typePaiement.libelle.includes('Droit d\'établissement') || 
        obligation.typePaiement.libelle.includes('DEA')) {
      return 'DEA';
    } else if (obligation.typePaiement.libelle.includes('Taxe superficiaire') || 
               obligation.typePaiement.libelle.includes('TS')) {
      return 'TS';
    } else if (obligation.typePaiement.libelle.includes('Produit d\'attribution') || 
               obligation.typePaiement.libelle.includes('Attribution')) {
      return 'PRODUIT_ATTRIBUTION';
    }
    return null;
  };


// Add this function to handle PDF download
const handlePdfDownload = (pdfBlob: Blob) => {
  const url = window.URL.createObjectURL(pdfBlob);
  const a = document.createElement('a');
  a.style.display = 'none';
  a.href = url;
  a.download = `Ordre_${currentPdfType}_${permisId}_${new Date().toISOString().split('T')[0]}.pdf`;
  document.body.appendChild(a);
  a.click();
  window.URL.revokeObjectURL(url);
  document.body.removeChild(a);
  setPreviewModalOpen(false);
};

const handleRegeneratePdf = async (newData: any): Promise<string> => {
  try {
    // Add current date to the data
    const dataWithDate = {
      ...newData,
      date: new Date()
    };
    
    // Regenerate the PDF with the new data
    const newPdfData = await generatePDFForPreview(currentPdfType, dataWithDate);
    return newPdfData;
  } catch (error) {
    console.error("Error regenerating PDF:", error);
    throw new Error("Erreur lors de la régénération du PDF");
  }
};
  
    return (
    <div className={styles['app-container']}>
      <Navbar />
      <div className={styles['app-content']}>
        <Sidebar currentView={currentView} navigateTo={navigateTo} />
        <main className={styles['main-content']}>
          <div className={styles['breadcrumb']}>
            <span>SIGAM</span>
            <FiChevronRight className={styles['breadcrumb-arrow']} />
            <span>Paiements</span>
          </div>
          <div className={styles['container']}>
                        <div className={styles['content-wrapper']}>
                         {/* Progress Steps */}
<div className={styles.progressBar}>
  <div className={styles.progressFill} style={{ width: `${progress}%` }}></div>
</div>
    
              <h1 className={styles.title}>Paiements requis</h1>
       

      {loading ? (
        <div className={styles.loading}>Chargement en cours...</div>
      ) : (
        <>
       <div className={styles['order-generation-section']}>
  <h2>Génération des ordres administratifs</h2>
  <p className={styles.instructions}>
    Générez les ordres de perception officiels pour chaque type de frais. Les documents seront formatés conformément aux standards administratifs.
  </p>
  
  <div className={styles['order-buttons']}>
    <button 
      className={styles['order-button']}
      onClick={() => handleGenerateOrder('DEA')}
      disabled={generatingOrder === 'DEA' || !obligations.some(ob => 
        ob.typePaiement.libelle.includes('Droit d\'établissement') || 
        ob.typePaiement.libelle.includes('DEA')
      )}
    >
      <FiDownload className={styles['button-icon']} />
      {generatingOrder === 'DEA' ? 'Génération...' : 'Ordre DEA'}
    </button>
    
    <button 
      className={styles['order-button']}
      onClick={() => handleGenerateOrder('TS')}
      disabled={generatingOrder === 'TS' || !obligations.some(ob => 
        ob.typePaiement.libelle.includes('Taxe superficiaire') || 
        ob.typePaiement.libelle.includes('TS')
      )}
    >
      <FiDownload className={styles['button-icon']} />
      {generatingOrder === 'TS' ? 'Génération...' : 'Ordre TS'}
    </button>
    
    <button 
      className={styles['order-button']}
      onClick={() => handleGenerateOrder('PRODUIT_ATTRIBUTION')}
      disabled={generatingOrder === 'PRODUIT_ATTRIBUTION' || !obligations.some(ob => 
        ob.typePaiement.libelle.includes('Produit d\'attribution') || 
        ob.typePaiement.libelle.includes('Attribution')
      )}
    >
      <FiDownload className={styles['button-icon']} />
      {generatingOrder === 'PRODUIT_ATTRIBUTION' ? 'Génération...' : 'Ordre Produit d\'Attribution'}
    </button>
  </div>
</div>
          <div className={styles.summarySection}>
        <h2>Résumé des paiements</h2>
        <table className={styles.summaryTable}>
          <thead>
            <tr>
              <th>Type de frais</th>
              <th>Montant (DZD)</th>
              <th>Période</th>
              <th>Référence</th>
              <th>Statut</th>
              <th>Télécharger</th>
            </tr>
          </thead>
          <tbody>
            {obligations.map((obligation) => {
              const obligationType = getObligationType(obligation);
              const isSelectedForDownload = obligationType && selectedForDownload[obligationType] === obligation.id;
              
              return (
                <tr
                  key={obligation.id}
                  className={selectedObligation?.id === obligation.id ? styles.selectedRow : ''}
                  onClick={() => handleObligationSelect(obligation)}
                >
                  <td>
                    <strong>{obligation.typePaiement.libelle}</strong>
                    <div className={styles.legalReference}>
                      {getLegalReference(obligation.typePaiement.id)}
                    </div>
                  </td>
                  <td>{obligation.amount.toLocaleString()} DZD</td>
                  <td>
                    {getPeriodDisplay(obligation)}
                  </td>
                  <td>
                    {obligation.typePaiement.libelle.substring(0, 2).toUpperCase()}-{obligation.id}
                  </td>
                  <td>
                    <span className={statusStyles[obligation.status]}>
                      {obligation.status}
                    </span>
                  </td>
                  <td>
                    {obligationType && (
                      <button
                        className={`${styles.downloadSelectButton} ${isSelectedForDownload ? styles.selected : ''}`}
                        onClick={(e) => {
                          e.stopPropagation();
                          handleSelectForDownload(obligation.id, obligationType);
                        }}
                        title="Sélectionner pour le téléchargement"
                      >
                        {isSelectedForDownload ? <FiCheck /> : <FiDownload />}
                      </button>
                    )}
                  </td>
                </tr>
              );
            })}
            <tr className={styles.totalRow}>
              <td colSpan={5}><strong>Total à payer</strong></td>
              <td><strong>{totalAmount.toLocaleString()} DZD</strong></td>
            </tr>
          </tbody>
        </table>
      </div>

          <div className={styles.paymentSection}>
            <h2>Saisie des preuves de paiement</h2>
            <p className={styles.instructions}>
              Pour chaque frais, saisissez les informations de paiement reçues du demandeur et uploadez les justificatifs officiels.
              Vérifiez la conformité avant validation.
            </p>

            {selectedObligation && (
              <>
                <div className={styles.obligationHeader}>
                  <h3>
                    {selectedObligation.typePaiement.libelle}
                    <span className={styles.amountDue}>
                      Montant dû»: {selectedObligation.amount.toLocaleString()} DZD • Réf: {selectedObligation.typePaiement.libelle.substring(0, 2).toUpperCase()}-{selectedObligation.id}
                    </span>
                  </h3>
                </div>

                <PaymentForm
                  obligation={selectedObligation}
                  onSubmit={handlePaymentSubmit}
                />

                {selectedObligation.payments && selectedObligation.payments.length > 0 && (
                  <PaymentsTable
                    payments={selectedObligation.payments.map((p) => ({
                      id: p.id,
                      date_paiement: p.paymentDate,
                      montant_paye: p.amount,
                      mode_paiement: p.paymentMethod,
                      num_quittance: p.receiptNumber,
                      etat_paiement: p.status,
                      justificatif_url: p.proofUrl,
                    }))}
                  />
                )}
                {permisDetails && (
              <TaxeSuperficiaireSection
                obligations={obligations}
                permis={permisDetails}
                dateAttribution={dateAttribution}
              />
            )}
              </>
            )}
          </div>
        </>
      )}
      </div>
      <div className={styles['navigation-buttons']}>
                            <button className={`${styles['btn']} ${styles['btn-outline']}`} onClick={handleBack} >
                                <FiChevronLeft className={styles['btn-icon']} />
                                Précédent
                            </button>
                            <PDFPreviewModal
  isOpen={previewModalOpen}
  onClose={() => setPreviewModalOpen(false)}
  pdfData={currentPdfData}
  onSave={handlePdfDownload}
  type={currentPdfType}
  orderData={currentOrderData}
  onRegeneratePdf={handleRegeneratePdf} // Add this prop
/>
                        </div>
                        
    </div>
    </main>
    </div>
    </div>
  );
};

function getLegalReference(paymentTypeId: number): string {
  switch (paymentTypeId) {
    case 1: return 'Article 45 du Code Minier';
    case 2: return 'Article 28 du Code Minier';
    case 3: return 'Article 33 Décret 07-154';
    default: return '';
  }
}

export default PaymentPage;
