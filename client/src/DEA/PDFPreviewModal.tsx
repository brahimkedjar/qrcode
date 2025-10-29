// components/PDFPreviewModal.tsx
import { useState, useRef, useEffect } from 'react';
import { FiX, FiDownload, FiEdit2, FiSave } from 'react-icons/fi';
import styles from './Payments.module.css';
import { generatePDFForPreview } from '../../utils/pdfGenerator';

interface PDFPreviewModalProps {
  isOpen: boolean;
  onClose: () => void;
  pdfData: string;
  onSave: (pdfBlob: Blob) => void;
  type: 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION';
  orderData: any;
  onRegeneratePdf: (newData: any) => Promise<string>;
}

const PDFPreviewModal: React.FC<PDFPreviewModalProps> = ({ 
  isOpen, 
  onClose, 
  pdfData, 
  onSave,
  type,
  orderData,
  onRegeneratePdf
}) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editedData, setEditedData] = useState<any>(null);
  const [currentPdfData, setCurrentPdfData] = useState(pdfData);
  const iframeRef = useRef<HTMLIFrameElement>(null);

  // Initialize editedData when orderData changes
  useEffect(() => {
    if (orderData) {
      setEditedData({
        ...orderData,
        // Ensure these fields have proper default values
        president: orderData.president || "P/Le Président du Comité de Direction",
        signatureName: orderData.signatureName || "Seddik BENABBES"
      });
    } else {
      // Set default values if orderData is null/undefined
      setEditedData({
        companyName: '',
        permitType: '',
        permitCode: '',
        location: '',
        amount: 0,
        orderNumber: '',
        taxReceiver: '',
        taxReceiverAddress: '',
        period: '',
        president: "P/Le Président du Comité de Direction",
        signatureName: "Seddik BENABBES"
      });
    }
  }, [orderData]);

  // Update currentPdfData when pdfData changes
  useEffect(() => {
    setCurrentPdfData(pdfData);
  }, [pdfData]);

  if (!isOpen || !editedData) return null;

  const handleDownload = () => {
    // Convert data URL to blob
    fetch(currentPdfData)
      .then(res => res.blob())
      .then(blob => {
        onSave(blob);
      });
  };

  const handleEditToggle = async () => {
    if (isEditing) {
      // Save mode - regenerate PDF with edited data
      try {
        const newPdfData = await onRegeneratePdf(editedData);
        setCurrentPdfData(newPdfData);
        setIsEditing(false);
      } catch (error) {
        console.error("Error regenerating PDF:", error);
        alert("Erreur lors de la régénération du PDF");
      }
    } else {
      // Edit mode - just toggle editing state
      setIsEditing(true);
    }
  };

  const handleFieldChange = (field: string, value: any) => {
    setEditedData((prev: any) => ({
      ...prev,
      [field]: value
    }));
  };

  return (
    <div className={styles.modalOverlay}>
      <div className={styles.modalContent}>
        <div className={styles.modalHeader}>
          <h2>
            Aperçu de l'ordre {type === 'DEA' ? 'DEA' : type === 'TS' ? 'Taxe Superficiaire' : 'Produit d\'Attribution'}
          </h2>
          <button className={styles.closeButton} onClick={onClose}>
            <FiX />
          </button>
        </div>

        <div className={styles.modalBody}>
          {isEditing ? (
            <div className={styles.editorContainer}>
              <div className={styles.editorForm}>
                <div className={styles.formGroup}>
                  <label>Nom de l'entreprise:</label>
                  <input
                    type="text"
                    value={editedData.companyName || ''}
                    onChange={(e) => handleFieldChange('companyName', e.target.value)}
                  />
                </div>
                
                <div className={styles.formGroup}>
                  <label>Type de permis:</label>
                  <input
                    type="text"
                    value={editedData.permitType || ''}
                    onChange={(e) => handleFieldChange('permitType', e.target.value)}
                  />
                </div>
                
                <div className={styles.formGroup}>
                  <label>Code de permis:</label>
                  <input
                    type="text"
                    value={editedData.permitCode || ''}
                    onChange={(e) => handleFieldChange('permitCode', e.target.value)}
                  />
                </div>
                
                <div className={styles.formGroup}>
                  <label>Localisation:</label>
                  <input
                    type="text"
                    value={editedData.location || ''}
                    onChange={(e) => handleFieldChange('location', e.target.value)}
                  />
                </div>
                
                <div className={styles.formGroup}>
                  <label>Montant (DA):</label>
                  <input
                    type="number"
                    value={editedData.amount || 0}
                    onChange={(e) => handleFieldChange('amount', Number(e.target.value))}
                  />
                </div>
                
                <div className={styles.formGroup}>
                  <label>Numéro d'ordre:</label>
                  <input
                    type="text"
                    value={editedData.orderNumber || ''}
                    onChange={(e) => handleFieldChange('orderNumber', e.target.value)}
                  />
                </div>
                
                {type === 'TS' && (
                  <div className={styles.formGroup}>
                    <label>Période:</label>
                    <input
                      type="text"
                      value={editedData.period || ''}
                      onChange={(e) => handleFieldChange('period', e.target.value)}
                    />
                  </div>
                )}
                
                <div className={styles.formGroup}>
                  <label>Receveur des impôts:</label>
                  <input
                    type="text"
                    value={editedData.taxReceiver || ''}
                    onChange={(e) => handleFieldChange('taxReceiver', e.target.value)}
                  />
                </div>
                
                <div className={styles.formGroup}>
                  <label>Adresse:</label>
                  <input
                    type="text"
                    value={editedData.taxReceiverAddress || ''}
                    onChange={(e) => handleFieldChange('taxReceiverAddress', e.target.value)}
                  />
                </div>
                
                {/* FIXED: Changed from "presidant" to "president" */}
                <div className={styles.formGroup}>
                  <label>Responsable:</label>
                  <input
                    type="text"
                    value={editedData.president || ''}
                    onChange={(e) => handleFieldChange('president', e.target.value)}
                  />
                </div>

                {/* FIXED: Ensure signatureName field works properly */}
                <div className={styles.formGroup}>
                  <label>Nom du signataire:</label>
                  <input
                    type="text"
                    value={editedData.signatureName || ''}
                    onChange={(e) => handleFieldChange('signatureName', e.target.value)}
                  />
                </div>
              </div>
              
              <div className={styles.previewContainer}>
                <iframe
                  ref={iframeRef}
                  src={currentPdfData}
                  className={styles.previewIframe}
                  title="PDF Preview"
                />
              </div>
            </div>
          ) : (
            <div className={styles.previewContainer}>
              <iframe
                src={currentPdfData}
                className={styles.previewIframe}
                title="PDF Preview"
              />
            </div>
          )}
        </div>

        <div className={styles.modalFooter}>
          <button 
            className={styles.editButton}
            onClick={handleEditToggle}
          >
            {isEditing ? <FiSave /> : <FiEdit2 />}
            {isEditing ? 'Sauvegarder' : 'Modifier'}
          </button>
          
          <button 
            className={styles.downloadButton}
            onClick={handleDownload}
          >
            <FiDownload />
            Télécharger
          </button>
        </div>
      </div>
    </div>
  );
};

export default PDFPreviewModal;