from generators.suppliers import SuppliersGenerator


def main():

    generator = SuppliersGenerator()

    df = generator.generate()

    generator.load(df)


if __name__ == "__main__":
    main()