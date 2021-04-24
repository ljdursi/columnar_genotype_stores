TREEFILE = variants.ts
WGS_UNANNO = unanno.wgs.vcf
WGS = wgs.vcf
WES = wes.vcf
EXOME = exons.bed.gz

all: $(WES)

install_requirements: vcfanno 
	pip3 install stdpopsim

vcfanno:
	curl -L -o ./vcfanno https://github.com/brentp/vcfanno/releases/download/v0.3.2/vcfanno_osx
	chmod 755 ./vcfanno

gnomad.vcf.gz:
	curl -o $@ https://gnomad-public-us-east-1.s3.amazonaws.com/release/2.1.1/vcf/exomes/gnomad.exomes.r2.1.1.sites.vcf.bgz
	tabix -p vcf $@

dbsnp.vcf.gz:
	curl -o $@ ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b151_GRCh37p13/VCF/00-All.vcf.gz
	curl -o $@.tbi ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b151_GRCh37p13/VCF/00-All.vcf.gz.tbi

$(TREEFILE):
	stdpopsim HomSap -g HapMapII_GRCh37 -o $@ -d OutOfAfrica_2T12 3 3

$(WGS_UNANNO): $(TREEFILE)
	tskit vcf $(TREEFILE) -P 2 > $@

$(WGS): $(WGS_UNANNO) $(EXOME) vcfanno conf.toml 
	./vcfanno conf.toml $(WGS_UNANNO) > $@

$(WES): $(WGS) $(EXOME)
	bedtools intersect -header -a $(WGS) -b $(EXOME) > $@

clean:
	- rm -f $(TREEFILE) $(WGS) $(WES)
