import sqlite from 'sqlite3';
import stream from 'stream';

export default class DBStream extends stream.Readable {

    constructor( opts ) {
        super({ objectMode: true });
        this.sql = opts.sql;
        this.db = new sqlite.Database( opts.db );
        this.stmt = this.db.prepare( this.sql );
        this.on( 'end', () => this.stmt.finalize( () => this.db.close() ));
    }

    _read() {

        let strm = this;
        this.stmt.get( function(err,result) {

            // If result is undefined, push null, which will end the stream.
            /*
             * Should have no backpressure problems,
             * since _read is only called when the downstream is
             * ready to fetch data
             */
            err ?
                strm.emit('error', err ) :
                strm.push( result || null);

       })
    }

}
