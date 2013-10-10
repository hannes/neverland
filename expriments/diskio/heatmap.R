generateHeatmap <- function(datadir,outfile,dbtitle,interesting) {
	rds <- paste0(datadir,"/datacache.rds")
	if (!file.exists(rds)) {
		read <- function(fname) {
			d <- read.csv(fname,sep="\t",stringsAsFactors=F)
			d$query <- toupper(sub(".sql.csv","",basename(fname),1,3))
			#d$filename <- basename(d$filename)
			d$filename <- sub("hannes/neverland/ssbm/","",d$filename,fixed=T)
			d
		}

		sizes <- read.csv(paste0(datadir,"/sizes.tsv"),header=F,sep="\t",stringsAsFactors=F)
		names(sizes) <- c("filename","filesize")

		sizes <- sizes[order(sizes$filename),]
		sizes$size_offset <- cumsum(as.numeric(sizes$filesize))-sizes$filesize

		d <- lapply(dir(datadir,pattern="sql.csv",full.names=T),read)
		d <- do.call("rbind", d)

		d$query <- factor(d$query)
		d$readsize <- d$size*4096
		d$offset <- d$offset*4096

		d <- d[d$mode=="mmapread",c("query","filename","offset","readsize")]
		d <- d[order(d$filename,d$offset),]
		d <- merge(d,sizes,by="filename")

		d$cumsum_offset <- d$size_offset + d$offset
		save(d,sizes,file=rds)
	} else {
		load(rds)
	}

	# limit to interesting queries
	d <- d[substring(d$query,1,3) %in% interesting,]
	d$query <- factor(d$query)

	library(scales)
	#newsize <- 59*1024 # megabyte
	#newsize <- 100 # for testing
	totalsize <- sum(as.numeric(sizes$filesize))
	newsize <- (totalsize/1024)/1024

	roundmerge <- function(d) {
		list(query=as.character(d$query[[1]]),blocks=unique(round(rescale(d$cumsum_offset,to=c(0,newsize),from=c(0,totalsize)))))
	}

	queryfile <- split(d,list(d$query))
	a <- lapply(queryfile,roundmerge)


	all <- rep(0,newsize)

	collectheat <- function(x) {
		aa <- all
		aa[x$blocks] <- aa[x$blocks]+1
		all <<- aa
	}

	dd <- lapply(a,collectheat)

	pdf(outfile,height=12, width=20)

	ymax <- length(a)
	y <- ymax
	yoff <- 0.5

	plot(c(0,newsize),c(0,ymax),type="n",yaxt='n',frame.plot=F,xlab="Data (MB)",ylab="Queries")

	title(paste0(dbtitle," & SSBM (SF100) File IO"),format(Sys.Date(), format="%Y-%m-%d"))

	# plot queries
	plotq <- function(x) {
		q <- x$query
		x<-x$blocks
		#rect(0, y-yoff, totalsize, y+yoff,col="gray95",border=NA)
		rect(x, y-yoff, x+1, y+yoff,col="black",border=NA)
		text(x=newsize*-0.02,y=y,label=q)
		y <<- y-1
		FALSE
	}
	ff <- lapply(a,plotq) 

	# plot heat
	all <- data.frame(ind=seq(1,newsize),heat=all)

	#yoff <- yoff*2

	#rect(0, y-yoff, totalsize, y+yoff,col="gray90",border=NA)

	indices <- all[all$heat > 1 & all$heat <= 5,"ind"]
	if (length(indices) > 0) rect(indices, y-yoff, indices+1, y+yoff,col="yellow",border=NA)

	indices <- all[all$heat > 5 & all$heat <= 7,"ind"]
	if (length(indices) > 0)rect(indices, y-yoff, indices+1, y+yoff,col="orange",border=NA)

	indices <- all[all$heat > 7,"ind"]
	if (length(indices) > 0) rect(indices, y-yoff, indices+1, y+yoff,col="red",border=NA)


	# some dividers
	files <- unique(round(rescale(sizes$size_offset,to=c(0,newsize),from=c(0,totalsize))))
	segments(y0=-0.5,y1=ymax+0.5,x0=files,col="gray90",lwd=0.5)

	dev.off()

	# # file sizes
	# # find ./farms/SF-100/ssbm-sf100/bat -type f | while read file; do echo -e "$file\t" `stat -c%s "$file"`; done > sizes.tsv

}

interesting <- c("Q03","Q05","Q07","Q11")

generateHeatmap("stapres-postgres/","heatmap-postgres.pdf","PostgreSQL",interesting)
generateHeatmap("stapres-monetdb/" ,"heatmap-monetdb.pdf" ,"MonetDB"   ,interesting)




# #! /usr/bin/stap --skip-badvars

# global traceon, filenames

# probe begin {
#  	printf("timestamp\tinode\tfilename\tmode\toffset\tsize\n")
# }

# # intercept open() syscall at the right time to learn inode nr for our files
# # http://lxr.free-electrons.com/source/fs/open.c?v=3.6

# probe kernel.function("finish_open").return {
# 	inode = $dentry->d_inode->i_ino
# 	if (@defined($file) && !([inode] in traceon)) {
# 		parent = @cast($file, "file")->f_path->dentry->d_parent;
# 		fname = __file_filename($file)
#  		path = reverse_path_walk(parent) ."/".fname
# 		 if (path =~ "^(.*/pgdata-sf100.*)$" ) {
# 			# printf("Tracing %d %s\n",inode,path)
# 			traceon[inode] = 1
# 			filenames[inode] = path
# 		}
# 	}
# }


# probe kernel.function("vfs_rename") {
# 	inode = $old_dentry->d_inode->i_ino
# 	if (inode && [inode] in traceon) {
# 		name = d_name($new_dentry)
# 		path = reverse_path_walk($new_dentry->d_parent) ."/".name
# 		filenames[inode] = path
# 		printf("%d\t%d\t%s\trename\t%d\t%d\n",gettimeofday_us(),inode,path,0,0)
# 	}
# }

# # bio doc: http://www.mjmwired.net/kernel/Documentation/block/biodoc.txt
# # kernel source: http://lxr.free-electrons.com/source/fs/mpage.c?v=3.9#L74

# probe kernel.function("submit_bio") {
# 	inode =__bio_ino($bio)
# 	if (inode && [inode] in traceon) {
#    		page_index = $bio->bi_io_vec->bv_page->index
# 		if ($rw == 0) {
# 			mode = "mmapread"
# 		} else {
# 			mode = "mmapwrite"
# 		}
# 		printf("%d\t%d\t%s\t%s\t%d\t%d\n",gettimeofday_us(),inode,filenames[inode],mode,page_index,$bio->bi_size/4096)
# 	}
# }

