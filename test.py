import logging
import asyncio


from polygonio.polygon_options import PolygonOptions




# ------------------ End of PolygonOptions Class ------------------ #

# Main guard for basic testing
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    async def main():
        poly = PolygonOptions()
        await poly.connect()
        # Example: fetch universal snapshot for AAPL
        snapshot = await poly.get_universal_snapshot("O:SPY251231C00623000")
        if snapshot:
            logging.info("Universal Snapshot fetched successfully.")
            print(snapshot.df)
        else:
            logging.info("No snapshot data received.")
        # Example: get theoretical price (placeholder)
        aggs = await poly.fetch_option_aggs("O:SPY251231C00623000")
        if not aggs.empty:
            logging.info("Aggs Data:")
            for item in aggs:
                logging.info(item)
        await poly.close()
    asyncio.run(main())