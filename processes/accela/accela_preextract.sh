# convert windows line endings to 
dos2unix *.txt

# strip off extra nonsense
rename -v 's/\d{12}_([_A-Z1-9]{1,})_[_\dA-Za-z-\]\[]{1,}/\1_original/' *.txt

# # handle each file
# 
# b1permit
cat B1PERMIT_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> B1PERMIT_fixed.txt
# b3addres - don't split
cat B3ADDRES_original.txt | iconv -f latin1 -t utf8 | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/$/"/g' >> B3ADDRES_fixed.txt
# b1_expiration
cat B1_EXPIRATION_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> B1_EXPIRATION.txt
# b3contact
cat B3CONTACT_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> B3CONTACT_fixed.txt
# B3CONTRA.txt
cat B3CONTRA_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> B3CONTRA_fixed.txt
# B3OWNERS.txt
cat B3OWNERS_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> B3OWNERS_fixed.txt
# B3PARCEL.txt
cat B3PARCEL_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> B3PARCEL_fixed.txt
# BCHCKBOX.txt
cat BCHCKBOX_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> BCHCKBOX_fixed.txt
# BPERMIT_DETAIL.txt
cat BPERMIT_DETAIL_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> BPERMIT_DETAIL_fixed.txt
# BWORKDES.txt
cat BWORKDES_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> BWORKDES_fixed.txt
# F4FEEITEM.txt
cat F4FEEITEM_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> F4FEEITEM_fixed.txt
# G6ACTION.txt
cat G6ACTION_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> G6ACTION_fixed.txt
# GPROCESS.txt
cat GPROCESS_original.txt | iconv -f latin1 -t utf8 | tr '\n' ' ' | sed 's/DETROIT|/\'$'\nDETROIT|/g' | sed 's/|/"|"/g' | sed 's/^/"/g' | sed 's/ $/"/g' >> GPROCESS_fixed.txt
